"""Kafka consumer that writes bronze Parquet files to MinIO.

Consumes events from ``raw.trades`` and ``raw.klines`` Kafka topics,
buffers rows in memory, and flushes to the MinIO bronze bucket as
Snappy-compressed Parquet files under a hive-style partition layout:

    bronze/
      trades/symbol=BTCUSDT/year=2026/month=07/day=05/<uuid>.parquet
      klines/symbol=BTCUSDT/interval=1m/year=2026/month=07/day=05/<uuid>.parquet

Flush is triggered by whichever threshold is reached first:
- ``lake_flush_rows`` records accumulated in a single topic buffer, or
- ``lake_flush_seconds`` elapsed since the last flush.

On ``SIGINT`` / ``SIGTERM`` the writer flushes all buffers before exit.
"""

from __future__ import annotations

import io
import json
import signal
import uuid
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from loguru import logger
from mypy_boto3_s3 import S3Client

from ingestion.config import IngestionConfig
from utils.storage import make_s3_client


class LakeWriter:
    """Consume Kafka topics and write bronze Parquet files to MinIO.

    Instantiate once, then call ``run()`` which blocks until a shutdown
    signal is received. All buffers are flushed and offsets committed
    before the process exits.

    Args:
        config: Resolved ``IngestionConfig`` instance.
    """

    def __init__(self, config: IngestionConfig) -> None:
        self._config = config
        self._consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "group.id": "lake-writer-bronze-v1",
                "auto.offset.reset": "earliest",
                # Manual commit: only after a successful MinIO flush.
                "enable.auto.commit": False,
            }
        )
        self._s3: S3Client = make_s3_client(
            endpoint=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
        )
        self._topics = [config.kafka_topic_trades, config.kafka_topic_klines]
        self._buffers: dict[str, list[dict[str, object]]] = {
            t: [] for t in self._topics
        }
        self._last_flush: datetime = datetime.now(UTC)
        self._running = True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _partition_key(
        self,
        *,
        topic: str,
        symbol: str,
        interval: str | None,
        now: datetime,
    ) -> str:
        """Build the hive-partitioned S3 object key.

        Args:
            topic: Source Kafka topic; used to derive the event folder name.
            symbol: Trading-pair symbol (e.g. ``BTCUSDT``).
            interval: Kline interval (e.g. ``1m``); ``None`` for trades.
            now: Timestamp used for year/month/day partitioning.

        Returns:
            S3 key string ending with a UUID filename.
        """
        event_folder = "trades" if "trade" in topic else "klines"
        date_part = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        symbol_part = f"symbol={symbol}"

        if interval:
            prefix = f"{event_folder}/{symbol_part}/interval={interval}/{date_part}"
        else:
            prefix = f"{event_folder}/{symbol_part}/{date_part}"

        return f"{prefix}/{uuid.uuid4()}.parquet"

    def _flush_topic(self, topic: str) -> None:
        """Write the in-memory buffer for ``topic`` to MinIO as Parquet.

        Groups rows by symbol so each object is symbol-partitioned.
        No-op if the buffer is empty.

        Args:
            topic: Kafka topic whose buffer should be flushed.
        """
        rows = self._buffers[topic]
        if not rows:
            return

        now = datetime.now(UTC)
        by_symbol: dict[str, list[dict[str, object]]] = {}
        for row in rows:
            sym = str(row.get("symbol", "UNKNOWN"))
            by_symbol.setdefault(sym, []).append(row)

        for symbol, records in by_symbol.items():
            interval: str | None = None
            if records and "kline" in records[0]:
                kline_data = records[0]["kline"]
                if isinstance(kline_data, dict):
                    interval = str(kline_data.get("interval", ""))

            table = pa.Table.from_pylist(records)
            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            buf.seek(0)

            key = self._partition_key(
                topic=topic,
                symbol=symbol,
                interval=interval,
                now=now,
            )
            self._s3.put_object(
                Bucket=self._config.minio_bucket_bronze,
                Key=key,
                Body=buf.getvalue(),
                ContentType="application/octet-stream",
            )
            logger.info(
                "Flushed {n} rows → MinIO | topic={topic} symbol={symbol} key={key}",
                n=len(records),
                topic=topic,
                symbol=symbol,
                key=key,
            )

        self._buffers[topic] = []

    def _should_flush(self) -> bool:
        """Return True if any flush threshold has been exceeded.

        Returns:
            True when elapsed time >= ``lake_flush_seconds`` OR any
            buffer contains >= ``lake_flush_rows`` rows.
        """
        elapsed = (datetime.now(UTC) - self._last_flush).total_seconds()
        if elapsed >= self._config.lake_flush_seconds:
            return True
        return any(
            len(buf) >= self._config.lake_flush_rows for buf in self._buffers.values()
        )

    def _flush_all(self) -> None:
        """Flush all topic buffers and reset the flush timer."""
        for topic in self._topics:
            self._flush_topic(topic)
        self._last_flush = datetime.now(UTC)

    def _handle_signal(self, signum: int, _frame: object) -> None:
        """Set the running flag to False on SIGINT or SIGTERM.

        Args:
            signum: The received signal number.
            _frame: Current stack frame (unused).
        """
        logger.info("Shutdown signal received | signal={signum}", signum=signum)
        self._running = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the consumer loop; blocks until a shutdown signal.

        Subscribes to the configured Kafka topics, polls for messages,
        buffers them, and flushes to MinIO when a threshold is crossed.
        Commits offsets only after a successful flush.

        Raises:
            KafkaException: On unrecoverable Kafka errors.
        """
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._consumer.subscribe(self._topics)
        logger.info(
            "Lake writer started | topics={topics} flush_rows={flush_rows}"
            " flush_seconds={flush_seconds}",
            topics=self._topics,
            flush_rows=self._config.lake_flush_rows,
            flush_seconds=self._config.lake_flush_seconds,
        )

        try:
            while self._running:
                msg: Message | None = self._consumer.poll(timeout=1.0)

                if msg is None:
                    if self._should_flush():
                        self._flush_all()
                        self._consumer.commit(asynchronous=False)
                    continue

                err = msg.error()
                if err is not None:
                    # _PARTITION_EOF is informational, not an error.
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(err)

                topic = msg.topic()
                if topic not in self._buffers:
                    logger.warning(
                        "Received message from unexpected topic | topic={topic}",
                        topic=topic,
                    )
                    continue

                raw_value = msg.value()
                if raw_value is None:
                    continue

                try:
                    payload: dict[str, object] = json.loads(raw_value.decode("utf-8"))
                    self._buffers[topic].append(payload)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    logger.warning(
                        "Failed to decode message | topic={topic} error={error}",
                        topic=topic,
                        error=str(exc),
                    )
                    continue

                if self._should_flush():
                    self._flush_all()
                    self._consumer.commit(asynchronous=False)

        finally:
            logger.info("Flushing remaining buffers before shutdown...")
            self._flush_all()
            try:
                self._consumer.commit(asynchronous=False)
            except KafkaException as exc:
                # Ignore if no offsets are stored to commit.
                if exc.args[0].code() != KafkaError._NO_OFFSET:
                    logger.warning(
                        "Offset commit failed on shutdown | error={err}",
                        err=str(exc),
                    )
            self._consumer.close()
            logger.info("Lake writer shut down cleanly")
