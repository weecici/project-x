"""Entrypoint for the PySpark bronze → silver kline transformer.

Run with:
    uv run silver
"""

from __future__ import annotations

import sys

from loguru import logger

from batch.config import BatchConfig
from batch.silver.kline_transformer import run_silver
from utils.logging import configure_logging


def main() -> None:
    """Run the PySpark silver transformer against the configured MinIO buckets."""
    configure_logging()
    config = BatchConfig()
    logger.info(
        "Silver job starting | bronze_bucket={bronze} silver_bucket={silver}",
        bronze=config.minio_bucket_bronze,
        silver=config.minio_bucket_silver,
    )
    run_silver(config)


def cli() -> None:
    """Synchronous CLI entrypoint (used by [project.scripts])."""
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Silver job stopped by user")
        sys.exit(0)


if __name__ == "__main__":
    cli()
