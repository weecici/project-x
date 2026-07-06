"""Entrypoint for the Kafka → MinIO bronze lake writer.

Run with:
    uv run ingestion/run_lake_writer.py
"""

from __future__ import annotations

import sys

from loguru import logger

from ingestion.config import IngestionConfig
from ingestion.producer.lake_writer import LakeWriter
from ingestion.utils.logging import configure_logging


def main() -> None:
    """Run the lake writer until a shutdown signal is received."""
    configure_logging()
    config = IngestionConfig()
    logger.info(
        "Starting lake writer | bucket={bucket}",
        bucket=config.minio_bucket_bronze,
    )

    writer = LakeWriter(config)
    writer.run()


def cli() -> None:
    """Synchronous CLI entrypoint (used by [project.scripts])."""
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Lake writer stopped by user")
        sys.exit(0)


if __name__ == "__main__":
    cli()
