"""Entrypoint for the MinIO silver → ClickHouse OLAP loader.

Run with:
    uv run load-olap
"""

from __future__ import annotations

import sys

from loguru import logger

from olap.config import OlapConfig
from olap.loader import load_klines
from utils.logging import configure_logging


def main() -> None:
    """Run the silver → ClickHouse load for all kline Parquet files."""
    configure_logging()
    config = OlapConfig()
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
