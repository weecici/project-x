"""Shared pytest fixtures for all test levels.

Fixtures defined here are available to every test module without import.
"""

from __future__ import annotations

import pytest

from ingestion.config import IngestionConfig


@pytest.fixture()
def base_config() -> IngestionConfig:
    """Return an IngestionConfig with fast-flush test defaults.

    Uses env-file=None to prevent loading a local .env during tests.
    """
    return IngestionConfig(
        _env_file=None,  # type: ignore[call-arg]
        kafka_bootstrap_servers="localhost:9094",
        minio_endpoint="http://localhost:9000",
        symbols=["BTCUSDT"],
        kline_intervals=["1m"],
        lake_flush_rows=10,
        lake_flush_seconds=5,
    )
