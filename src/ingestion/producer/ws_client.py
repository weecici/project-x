"""Async Binance WebSocket → Kafka producer.

Subscribes to the Binance combined-stream WebSocket endpoint for the
configured symbols and kline intervals, validates each event with
Pydantic, and publishes normalised JSON payloads to Kafka.

Failed validation sends the raw payload to the dead-letter topic so no
data is silently dropped.
"""

from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

import websockets
from confluent_kafka import Producer
from loguru import logger
from pydantic import ValidationError

from ingestion.config import IngestionConfig
from ingestion.models import KlineEvent, TradeEvent
from ingestion.utils.retry import async_retry

if TYPE_CHECKING:
    pass

# Thread pool for off-loading blocking confluent-kafka calls.
# Two threads are sufficient: one for produce(), one for poll().
_EXECUTOR: ThreadPoolExecutor = ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="kafka-producer",
)


class BinanceWSProducer:
    """Stream Binance WebSocket events to Kafka topics.

    Connects to the Binance combined-stream endpoint, parses trade and
    kline events, validates them with Pydantic, and publishes to the
    configured Kafka topics. Reconnects automatically on disconnection.

    Usage::

        config = IngestionConfig()
        producer = BinanceWSProducer(config)
        try:
            await producer.run()
        finally:
            await producer.close()
    """

    def __init__(self, config: IngestionConfig) -> None:
        self._config = config
        self._producer = Producer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                # Idempotent producer: exactly-once delivery per session.
                "enable.idempotence": True,
                "acks": "all",
                # Small linger reduces per-message latency for a live stream.
                "linger.ms": 5,
                "batch.size": 16_384,
                "compression.type": "snappy",
            }
        )
        self._loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _produce(
        self,
        *,
        topic: str,
        key: str,
        value: dict[str, Any],
    ) -> None:
        """Produce a single JSON message to Kafka asynchronously.

        Wraps the blocking confluent-kafka ``produce()`` call in a
        thread-pool executor so the event loop is not blocked.

        Args:
            topic: Destination Kafka topic.
            key: Partition key (trading pair symbol).
            value: Message payload; will be JSON-serialised.
        """
        if self._loop is None:
            self._loop = asyncio.get_running_loop()

        payload = json.dumps(value).encode("utf-8")
        key_bytes = key.encode("utf-8")

        await self._loop.run_in_executor(
            _EXECUTOR,
            lambda: self._producer.produce(
                topic=topic,
                key=key_bytes,
                value=payload,
                on_delivery=self._on_delivery,
            ),
        )
        # Poll immediately to trigger delivery callbacks without blocking.
        await self._loop.run_in_executor(_EXECUTOR, self._producer.poll, 0)

    @staticmethod
    def _on_delivery(err: object, msg: object) -> None:
        """Handle Kafka producer delivery callback.

        Args:
            err: Delivery error object, or None on success.
            msg: The delivered Kafka message object.
        """
        if err is not None:
            logger.error("Kafka delivery failed | error={err}", err=str(err))
        else:
            logger.debug(
                "Message delivered | topic={topic} partition={partition}",
                topic=getattr(msg, "topic", lambda: "?")(),
                partition=getattr(msg, "partition", lambda: -1)(),
            )

    async def _route_message(self, raw_data: dict[str, Any]) -> None:
        """Parse and route a single Binance event to its Kafka topic.

        Validates the event with the appropriate Pydantic model. On
        success, publishes the normalised payload. On failure, publishes
        the raw payload to the dead-letter topic.

        Args:
            raw_data: Parsed JSON dict from the WebSocket stream.
        """
        event_type: str = raw_data.get("e", "")

        if event_type == "trade":
            try:
                trade_event = TradeEvent.model_validate(raw_data)
                await self._produce(
                    topic=self._config.kafka_topic_trades,
                    key=trade_event.symbol,
                    value=trade_event.model_dump(mode="json"),
                )
            except ValidationError as exc:
                logger.warning(
                    "Trade validation failed; DLQ routing | errors={errors} "
                    "details={details}",
                    errors=exc.error_count(),
                    details=exc.errors(),
                )
                await self._produce(
                    topic=self._config.kafka_dlq_trades,
                    key="unknown",
                    value=raw_data,
                )

        elif event_type == "kline":
            try:
                kline_event = KlineEvent.model_validate(raw_data)
                await self._produce(
                    topic=self._config.kafka_topic_klines,
                    key=kline_event.symbol,
                    value=kline_event.model_dump(mode="json"),
                )
            except ValidationError as exc:
                logger.warning(
                    "Kline validation failed; routing to DLQ | errors={errors}",
                    errors=exc.error_count(),
                )
                await self._produce(
                    topic=self._config.kafka_dlq_klines,
                    key="unknown",
                    value=raw_data,
                )

        else:
            logger.debug(
                "Ignoring unknown event type | event_type={event_type}",
                event_type=event_type,
            )

    def _build_stream_url(self) -> str:
        """Build the Binance combined-stream WebSocket URL.

        Returns:
            Full ``wss://`` URL for the combined stream endpoint, with
            one stream per (symbol x interval) pair plus a trade stream
            per symbol.
        """
        streams: list[str] = []
        for symbol in self._config.symbols:
            sym = symbol.lower()
            streams.append(f"{sym}@trade")
            for interval in self._config.kline_intervals:
                streams.append(f"{sym}@kline_{interval}")

        base_url = self._config.binance_ws_base_url.rstrip("/")
        if base_url.endswith("/ws"):
            base_url = base_url[:-3]

        return f"{base_url}/stream?streams=" + "/".join(streams)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @async_retry(max_attempts=10, min_wait=2, max_wait=60)
    async def run(self) -> None:
        """Connect to Binance WebSocket and stream events to Kafka.

        Runs indefinitely, reconnecting automatically on connection loss.
        The ``@async_retry`` decorator handles exponential backoff
        between reconnection attempts.

        Raises:
            KafkaException: If Kafka produce fails after exhausting retries.
            websockets.exceptions.WebSocketException: Propagated after
                all reconnection attempts are exhausted.
        """
        url = self._build_stream_url()
        logger.info(
            "Connecting to Binance WebSocket | url={url}",
            url=url,
        )

        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=10,
            open_timeout=30,
        ) as ws:
            logger.info("WebSocket connection established")
            async for message in ws:
                if isinstance(message, bytes):
                    message = message.decode("utf-8")

                try:
                    parsed: dict[str, Any] = json.loads(message)
                except json.JSONDecodeError:
                    logger.warning(
                        "Non-JSON WebSocket frame, skipping | preview={preview}",
                        preview=message[:120],
                    )
                    continue

                # Combined stream wraps events: {"stream": "...", "data": {...}}
                data: dict[str, Any] = parsed.get("data", parsed)
                await self._route_message(data)

    async def close(self) -> None:
        """Flush in-flight messages and release resources.

        Waits up to 10 seconds for all pending Kafka deliveries to
        complete before shutting down the thread pool.
        """
        logger.info("Flushing Kafka producer (timeout=10s)...")
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        remaining = await self._loop.run_in_executor(
            _EXECUTOR, self._producer.flush, 10
        )
        if remaining > 0:
            logger.warning(
                "{n} message(s) not delivered before shutdown",
                n=remaining,
            )
        _EXECUTOR.shutdown(wait=False)
        logger.info("Kafka producer closed")
