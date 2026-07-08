"""Entrypoint for the Binance WebSocket → Kafka producer.

Run with:
    uv run produce
"""

from __future__ import annotations

import asyncio
import sys

from loguru import logger

from ingestion.config import IngestionConfig
from ingestion.producer.ws_client import BinanceWSProducer
from utils.logging import configure_logging


async def main() -> None:
    """Run the Binance WebSocket producer until interrupted."""
    configure_logging()
    config = IngestionConfig()
    logger.info(
        "Starting Binance WebSocket producer | symbols={symbols} intervals={intervals}",
        symbols=config.symbols,
        intervals=config.kline_intervals,
    )

    producer = BinanceWSProducer(config)
    try:
        await producer.run()
    finally:
        await producer.close()


def cli() -> None:
    """Synchronous CLI entrypoint (used by [project.scripts])."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
        sys.exit(0)


if __name__ == "__main__":
    cli()
