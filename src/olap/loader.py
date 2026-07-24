"""Silver Parquet → ClickHouse bulk loader.

Uses PyArrow's Dataset API to natively read Hive-partitioned Parquet files
from MinIO and inserts them into ClickHouse using the zero-copy Arrow columnar path.
"""

from __future__ import annotations

import sys

import clickhouse_connect
import pyarrow as pa
import pyarrow.dataset as ds
from loguru import logger
from pyarrow.fs import S3FileSystem  # type: ignore[attr-defined]

from olap.config import OlapLoaderConfig
from utils.logging import configure_logging

# DDL for raw kline table inside the silver database.
# Engine: ReplacingMergeTree(_loaded_at)
#   Deduplicates by PRIMARY KEY (symbol, interval, open_time) on background merge,
#   keeping the row with the highest _loaded_at. Idempotent re-loads are safe.
# Partitioning: (symbol, toYYYYMM(open_time))
#   Keeps partition files small; enables partition-level pruning in gold models.
KLINES_RAW_DDL = """
CREATE TABLE IF NOT EXISTS silver.klines_raw
(
    symbol                  LowCardinality(String),
    interval                LowCardinality(String),
    open_time               DateTime64(3, 'UTC'),
    open                    Decimal(18, 8),
    high                    Decimal(18, 8),
    low                     Decimal(18, 8),
    close                   Decimal(18, 8),
    volume                  Decimal(18, 8),
    close_time              DateTime64(3, 'UTC'),
    quote_volume            Decimal(18, 8),
    num_trades              UInt32,
    taker_buy_base_volume   Decimal(18, 8),
    taker_buy_quote_volume  Decimal(18, 8),
    _loaded_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY (symbol, toYYYYMM(open_time))
ORDER BY (symbol, interval, open_time)
SETTINGS index_granularity = 8192
"""

_INSERT_COLUMNS = [
    "symbol",
    "interval",
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "num_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
]

_SILVER_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("interval", pa.string()),
        pa.field("open_time", pa.timestamp("ms", tz="UTC")),
        pa.field("open", pa.decimal128(18, 8)),
        pa.field("high", pa.decimal128(18, 8)),
        pa.field("low", pa.decimal128(18, 8)),
        pa.field("close", pa.decimal128(18, 8)),
        pa.field("volume", pa.decimal128(18, 8)),
        pa.field("close_time", pa.timestamp("ms", tz="UTC")),
        pa.field("quote_volume", pa.decimal128(18, 8)),
        pa.field("num_trades", pa.int64()),
        pa.field("taker_buy_base_volume", pa.decimal128(18, 8)),
        pa.field("taker_buy_quote_volume", pa.decimal128(18, 8)),
    ]
)


def load_klines(config: OlapLoaderConfig) -> int:
    """Read all silver kline Parquet files from MinIO and load into ClickHouse.

    Uses pyarrow.dataset to read Hive-style partitioned folders, automatically
    mapping partition keys ('symbol', 'interval') to columns, avoiding manual
    path parsing.
    """
    client = clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
    )

    client.command("CREATE DATABASE IF NOT EXISTS silver")
    client.command("CREATE DATABASE IF NOT EXISTS gold")
    client.command(KLINES_RAW_DDL)

    # Initialize filesystem pointing to MinIO
    s3_fs = S3FileSystem(
        endpoint_override=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        scheme="http",
    )

    dataset_path = f"{config.minio_bucket_silver}/{config.silver_klines_prefix}"

    try:
        # Define dataset with Hive partitioning
        dataset = ds.dataset(  # type: ignore[no-untyped-call]
            dataset_path,
            filesystem=s3_fs,
            format="parquet",
            partitioning="hive",
        )

        # Load table with correct columns projection and schema casting
        table = dataset.to_table(columns=_INSERT_COLUMNS).cast(_SILVER_SCHEMA)
    except (FileNotFoundError, OSError):
        logger.warning(
            "No Parquet files found under s3://{bucket}/{prefix}",
            bucket=config.minio_bucket_silver,
            prefix=config.silver_klines_prefix,
        )
        return 0

    total_rows = int(table.num_rows)
    if total_rows == 0:
        logger.warning(
            "No Parquet files found under s3://{bucket}/{prefix}",
            bucket=config.minio_bucket_silver,
            prefix=config.silver_klines_prefix,
        )
        return 0

    logger.info(
        "Inserting Arrow table | rows={n} table=silver.{table}",
        n=total_rows,
        table=config.clickhouse_table_klines,
    )

    client.insert_arrow(
        f"silver.{config.clickhouse_table_klines}",
        table,
    )

    logger.info(
        "OLAP load complete | total_rows={n} table=silver.{table}",
        n=total_rows,
        table=config.clickhouse_table_klines,
    )
    return total_rows


def main() -> None:
    """Run the silver → ClickHouse load for all kline Parquet files."""
    configure_logging()
    config = OlapLoaderConfig()
    logger.info(
        "OLAP loader starting | host={host}:{port} db={db} bucket={bucket}",
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        db=config.clickhouse_db,
        bucket=config.minio_bucket_silver,
    )
    total = load_klines(config)
    logger.info("Done | total_rows_inserted={n}", n=total)


def cli() -> None:
    """Synchronous CLI entrypoint (used by [project.scripts])."""
    try:
        main()
    except KeyboardInterrupt:
        logger.info("OLAP loader stopped by user")
        sys.exit(0)


if __name__ == "__main__":
    cli()
