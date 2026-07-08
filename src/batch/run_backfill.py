"""Entrypoint for the Binance REST → MinIO bronze backfill.

Run with:
    uv run backfill
"""

from __future__ import annotations

import asyncio
import sys

from loguru import logger

from batch.backfill.binance_rest import run_backfill
from batch.config import BatchConfig
from utils.logging import configure_logging


async def main() -> None:
    """Run the historical kline backfill for all configured symbols/intervals."""
    configure_logging()
    config = BatchConfig()
    logger.info(
        "Backfill job starting | symbols={symbols} intervals={intervals}"
        " start={start} end={end}",
        symbols=config.symbols,
        intervals=config.kline_intervals,
        start=config.backfill_start_date,
        end=config.backfill_end_date or "today",
    )
    await run_backfill(config)


def cli() -> None:
    """Synchronous CLI entrypoint (used by [project.scripts])."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Backfill stopped by user")
        sys.exit(0)


if __name__ == "__main__":
    cli()
