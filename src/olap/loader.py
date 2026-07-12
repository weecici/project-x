"""Silver Parquet → ClickHouse bulk loader.

Uses PyArrow's Dataset API to natively read Hive-partitioned Parquet files
from MinIO and inserts them into ClickHouse using the zero-copy Arrow columnar path.
"""

from __future__ import annotations

import clickhouse_connect
import pyarrow as pa
import pyarrow.dataset as ds
from loguru import logger
from pyarrow.fs import S3FileSystem  # type: ignore[attr-defined]

from olap.config import OlapConfig
from olap.schema import KLINES_RAW_DDL

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


def load_klines(config: OlapConfig) -> int:
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

    # Define dataset with Hive partitioning
    dataset = ds.dataset(  # type: ignore[no-untyped-call]
        dataset_path,
        filesystem=s3_fs,
        format="parquet",
        partitioning="hive",
    )

    # Load table with correct columns projection and schema casting
    table = dataset.to_table(columns=_INSERT_COLUMNS).cast(_SILVER_SCHEMA)

    total_rows = int(table.num_rows)
    if total_rows == 0:
        logger.warning(
            "No Parquet files found under s3://{bucket}/{prefix}",
            bucket=config.minio_bucket_silver,
            prefix=config.silver_klines_prefix,
        )
        return 0

    logger.info(
        "Inserting Arrow table | rows={n} table={db}.{table}",
        n=total_rows,
        db=config.clickhouse_db,
        table=config.clickhouse_table_klines,
    )

    client.insert_arrow(
        f"{config.clickhouse_db}.{config.clickhouse_table_klines}",
        table,
    )

    logger.info(
        "OLAP load complete | total_rows={n} table={db}.{table}",
        n=total_rows,
        db=config.clickhouse_db,
        table=config.clickhouse_table_klines,
    )
    return total_rows
