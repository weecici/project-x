"""Entrypoint for the PySpark Structured Streaming OHLCV aggregator.

Run with:
    uv run stream-ohlcv
"""

from __future__ import annotations

import sys

from loguru import logger

from streaming.config import StreamingConfig
from streaming.jobs.ohlcv_stream import run_ohlcv_stream
from utils.logging import configure_logging


def main() -> None:
    """Initialize and run the OHLCV streaming queries."""
    configure_logging()
    config = StreamingConfig()

    logger.info(
        "Starting OHLCV stream job | bootstrap={bootstrap} topic={topic}",
        bootstrap=config.kafka_bootstrap_servers,
        topic=config.kafka_topic_klines,
    )

    from pyspark.sql import SparkSession

    _ = run_ohlcv_stream(config)

    logger.info("OHLCV streaming queries active. Blocked on termination...")

    # Wait for any query to terminate (either by crash or user exit)
    spark = SparkSession.getActiveSession()
    if spark is not None:
        spark.streams.awaitAnyTermination()


def cli() -> None:
    """Synchronous CLI entrypoint (used by [project.scripts])."""
    try:
        main()
    except KeyboardInterrupt:
        logger.info("OHLCV stream job terminated by user via SIGINT")
        sys.exit(0)
    except Exception as exc:
        logger.exception(
            "Fatal error in OHLCV stream job execution | {exc}", exc=str(exc)
        )
        sys.exit(1)


if __name__ == "__main__":
    cli()
