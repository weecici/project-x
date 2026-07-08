"""Binance REST API client for historical kline backfill.

Fetches kline (candlestick) data from ``GET /api/v3/klines`` using
paginated time-range requests. Each 1 000-bar chunk is validated with
``KlineRow``, serialised to Snappy-compressed Parquet, and written to
the MinIO bronze bucket under the same Hive-partition layout used by the
Phase 1 live writer:

    bronze/klines/symbol=X/interval=Y/year=Z/month=M/day=D/<uuid>.parquet

Rate-limit awareness:
    Binance enforces 1 200 request-weight per minute for unauthenticated
    callers. Each ``/api/v3/klines`` call costs weight 2. The client reads
    the ``X-MBX-USED-WEIGHT-1M`` response header and pauses when usage
    reaches 80 % of the limit (960/min), resuming after a calculated
    sleep interval.

Retries:
    Transient HTTP errors (5xx, timeouts) are retried via the shared
    ``utils.retry.async_retry`` decorator with exponential jitter backoff.
"""

from __future__ import annotations

import io
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from mypy_boto3_s3 import S3Client
from pydantic import ValidationError

from batch.config import BatchConfig
from batch.models import KlineRow
from utils.retry import async_retry
from utils.storage import make_s3_client

# Binance public rate-limit: 1 200 weight/min (unauthenticated).
# Pause when reaching 80 % to leave headroom.
_RATE_LIMIT_WEIGHT = 1200
_RATE_LIMIT_PAUSE_THRESHOLD = int(_RATE_LIMIT_WEIGHT * 0.8)
_KLINES_WEIGHT = 2  # weight cost per /api/v3/klines call
_MAX_BARS_PER_REQUEST = 1000

# PyArrow schema for kline rows — matches KlineRow fields exactly.
_KLINE_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("interval", pa.string()),
        pa.field("open_time", pa.int64()),
        pa.field("open", pa.string()),
        pa.field("high", pa.string()),
        pa.field("low", pa.string()),
        pa.field("close", pa.string()),
        pa.field("volume", pa.string()),
        pa.field("close_time", pa.int64()),
        pa.field("quote_volume", pa.string()),
        pa.field("num_trades", pa.int64()),
        pa.field("taker_buy_base_volume", pa.string()),
        pa.field("taker_buy_quote_volume", pa.string()),
    ]
)


def _date_to_ms(d: date) -> int:
    """Convert a UTC date to its millisecond epoch at midnight.

    Args:
        d: The date to convert.

    Returns:
        Millisecond timestamp for 00:00:00 UTC on ``d``.
    """
    return int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp() * 1000)


def _partition_key(*, symbol: str, interval: str, open_time_ms: int) -> str:
    """Build the hive-partitioned S3 key for a kline chunk.

    The key format matches the Phase 1 live writer so both data sources
    are co-located under the same prefix and queryable together.

    Args:
        symbol: Trading-pair symbol (e.g. ``BTCUSDT``).
        interval: Kline interval string (e.g. ``1m``).
        open_time_ms: Millisecond epoch of the first bar in the chunk —
            used to derive year/month/day partition values.

    Returns:
        S3 key string ending with a UUID filename.
    """
    ts = datetime.fromtimestamp(open_time_ms / 1000, tz=UTC)
    date_part = f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}"
    return (
        f"klines/symbol={symbol}/interval={interval}/{date_part}/{uuid.uuid4()}.parquet"
    )


def _rows_to_parquet(rows: list[KlineRow]) -> bytes:
    """Serialise a list of KlineRow objects to Snappy-compressed Parquet bytes.

    Decimal fields are stored as strings to preserve exact precision; the
    PySpark silver job casts them to ``DecimalType(18, 8)`` at read time.

    Args:
        rows: Validated kline bars to serialise.

    Returns:
        Raw Parquet bytes with Snappy compression.
    """
    records = [
        {
            "symbol": r.symbol,
            "interval": r.interval,
            "open_time": r.open_time,
            "open": str(r.open),
            "high": str(r.high),
            "low": str(r.low),
            "close": str(r.close),
            "volume": str(r.volume),
            "close_time": r.close_time,
            "quote_volume": str(r.quote_volume),
            "num_trades": r.num_trades,
            "taker_buy_base_volume": str(r.taker_buy_base_volume),
            "taker_buy_quote_volume": str(r.taker_buy_quote_volume),
        }
        for r in rows
    ]
    table = pa.Table.from_pylist(records, schema=_KLINE_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")  # type: ignore[no-untyped-call]
    buf.seek(0)
    return buf.getvalue()


@async_retry(max_attempts=5, min_wait=2, max_wait=60)
async def fetch_klines(
    client: httpx.AsyncClient,
    *,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = _MAX_BARS_PER_REQUEST,
) -> tuple[list[KlineRow], int]:
    """Fetch one page of kline bars from the Binance REST API.

    Args:
        client: Shared ``httpx.AsyncClient`` instance.
        symbol: Trading-pair symbol (e.g. ``BTCUSDT``).
        interval: Kline interval string (e.g. ``1m``).
        start_ms: Start timestamp in milliseconds (inclusive).
        end_ms: End timestamp in milliseconds (exclusive).
        limit: Number of bars to request per call (max 1 000).

    Returns:
        A tuple of ``(validated_rows, used_weight)`` where ``used_weight``
        is the cumulative 1-minute weight read from the response header.

    Raises:
        httpx.HTTPStatusError: On 4xx/5xx responses after retries.
        pydantic.ValidationError: If a row fails model validation.
    """
    response = await client.get(
        "/api/v3/klines",
        params={
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        },
    )
    response.raise_for_status()
    used_weight = int(response.headers.get("X-MBX-USED-WEIGHT-1M", "0"))

    raw: list[list[Any]] = response.json()
    rows: list[KlineRow] = []
    skipped = 0
    for item in raw:
        try:
            rows.append(KlineRow.from_api_list(item, symbol=symbol, interval=interval))
        except (ValidationError, ValueError) as exc:
            skipped += 1
            logger.warning(
                "Skipping malformed kline row | symbol={symbol} error={err}",
                symbol=symbol,
                err=str(exc),
            )

    if skipped:
        logger.warning(
            "Skipped {n} malformed rows | symbol={symbol} interval={interval}",
            n=skipped,
            symbol=symbol,
            interval=interval,
        )

    return rows, used_weight


async def backfill_symbol_interval(
    *,
    config: BatchConfig,
    s3: S3Client,
    symbol: str,
    interval: str,
) -> int:
    """Fetch and store all historical klines for one symbol/interval pair.

    Paginates the full date range in ``_MAX_BARS_PER_REQUEST``-bar chunks.
    Each chunk is written to S3 immediately after fetching. Rate-limit
    headers are monitored and the client sleeps when usage exceeds 80 %.

    Args:
        config: Resolved ``BatchConfig`` instance.
        s3: Configured S3 client (from ``utils.storage.make_s3_client``).
        symbol: Trading-pair symbol to backfill.
        interval: Kline interval to backfill.

    Returns:
        Total number of rows written to the bronze bucket.
    """
    start_date = date.fromisoformat(config.backfill_start_date)
    end_date = (
        date.fromisoformat(config.backfill_end_date)
        if config.backfill_end_date
        else date.today()
    )

    current_ms = _date_to_ms(start_date)
    end_ms = _date_to_ms(end_date + timedelta(days=1))  # end is exclusive

    total_rows = 0
    headers: dict[str, str] = {}
    if config.binance_api_key:
        headers["X-MBX-APIKEY"] = config.binance_api_key

    async with httpx.AsyncClient(
        base_url=config.binance_rest_base_url,
        headers=headers,
        timeout=httpx.Timeout(30.0),
    ) as client:
        while current_ms < end_ms:
            rows, used_weight = await fetch_klines(
                client,
                symbol=symbol,
                interval=interval,
                start_ms=current_ms,
                end_ms=end_ms,
            )

            if not rows:
                break

            # Write chunk to bronze.
            parquet_bytes = _rows_to_parquet(rows)
            key = _partition_key(
                symbol=symbol,
                interval=interval,
                open_time_ms=rows[0].open_time,
            )
            s3.put_object(
                Bucket=config.minio_bucket_bronze,
                Key=key,
                Body=parquet_bytes,
                ContentType="application/octet-stream",
            )
            total_rows += len(rows)
            logger.info(
                "Wrote {n} bars → bronze | "
                "symbol={symbol} interval={interval} key={key}",
                n=len(rows),
                symbol=symbol,
                interval=interval,
                key=key,
            )

            # Advance cursor: next page starts just after the last close_time.
            current_ms = rows[-1].close_time + 1

            # Rate-limit guard: pause when 80 % of the 1-min weight is consumed.
            if used_weight >= _RATE_LIMIT_PAUSE_THRESHOLD:
                import asyncio

                pause_s = 60 / (_RATE_LIMIT_WEIGHT / _KLINES_WEIGHT)
                logger.info(
                    "Rate-limit threshold reached (weight={w}); sleeping {s:.1f}s",
                    w=used_weight,
                    s=pause_s,
                )
                await asyncio.sleep(pause_s)

    logger.info(
        "Backfill complete | symbol={symbol} interval={interval} total_rows={n}",
        symbol=symbol,
        interval=interval,
        n=total_rows,
    )
    return total_rows


async def run_backfill(config: BatchConfig) -> None:
    """Run the full historical backfill for all configured symbols and intervals.

    Iterates over every (symbol, interval) combination in ``config`` and
    calls ``backfill_symbol_interval`` for each. Progress is logged at
    each chunk. All chunks are written directly to the MinIO bronze bucket.

    Args:
        config: Resolved ``BatchConfig`` instance.
    """
    s3 = make_s3_client(
        endpoint=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
    )
    logger.info(
        "Starting backfill | symbols={symbols} intervals={intervals}"
        " start={start} end={end}",
        symbols=config.symbols,
        intervals=config.kline_intervals,
        start=config.backfill_start_date,
        end=config.backfill_end_date or "today",
    )
    grand_total = 0
    for symbol in config.symbols:
        for interval in config.kline_intervals:
            rows_written = await backfill_symbol_interval(
                config=config, s3=s3, symbol=symbol, interval=interval
            )
            grand_total += rows_written

    logger.info("All backfill jobs complete | grand_total_rows={n}", n=grand_total)
