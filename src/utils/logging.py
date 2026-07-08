"""Structured logging configuration using loguru.

Call ``configure_logging()`` once at application startup before any
other logging calls. All subsequent ``logger`` usage from any module
will inherit this configuration.
"""

from __future__ import annotations

import sys

from loguru import logger

__all__ = ["configure_logging", "logger"]


def configure_logging(*, level: str = "INFO", serialize: bool = True) -> None:
    """Configure loguru for structured JSON output to stderr.

    Args:
        level: Minimum log level to emit (DEBUG, INFO, WARNING, ERROR).
        serialize: If True, emit JSON log records; if False, emit
            human-readable colourised text (useful during development).
    """
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DDTHH:mm:ss.SSS}Z</green> | "
            "<level>{level:<8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "{message}"
        ),
        serialize=serialize,
        backtrace=True,
        # Disable diagnose in production to avoid leaking local variable values.
        diagnose=False,
        colorize=not serialize,
    )
