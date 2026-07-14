"""E2E smoke tests for the Phase 2 batch pipeline.

Tests the full pipeline:
    Binance REST → MinIO bronze → PySpark silver

Requires: ``docker compose up -d`` with MinIO running.
Real Binance REST API is called (public endpoint, no key needed).
A 7-day window for BTCUSDT/1h is used to keep runtime manageable.

Mark: ``e2e`` — skip in CI unless explicitly enabled.
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta

import pytest

from batch.backfill.binance_rest import backfill_symbol_interval
from batch.config import BatchConfig
from batch.silver.kline_transformer import run_silver
from utils.storage import make_s3_client


@pytest.fixture(scope="module")
def e2e_config() -> BatchConfig:
    """BatchConfig targeting the local compose stack for E2E."""
    end = date.today()
    start = end - timedelta(days=7)
    return BatchConfig(
        _env_file=None,  # type: ignore[call-arg]
        symbols=["BTCUSDT"],
        kline_intervals=["1h"],
        backfill_start_date=start.isoformat(),
        backfill_end_date=end.isoformat(),
        minio_endpoint="http://localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_bucket_bronze="bronze",
        minio_bucket_silver="silver",
        spark_driver_memory="512m",
        spark_executor_memory="512m",
    )


@pytest.mark.e2e
class TestPhase2Pipeline:
    """Full E2E: REST backfill → bronze → silver."""

    def test_backfill_writes_bronze_parquet(self, e2e_config: BatchConfig) -> None:
        """Running backfill should write at least one Parquet to bronze."""
        s3 = make_s3_client(
            endpoint=e2e_config.minio_endpoint,
            access_key=e2e_config.minio_access_key,
            secret_key=e2e_config.minio_secret_key,
        )

        rows_written = asyncio.run(
            backfill_symbol_interval(
                config=e2e_config,
                s3=s3,
                symbol="BTCUSDT",
                interval="1h",
            )
        )

        assert rows_written > 0, "Expected at least one kline row to be fetched"

        response = s3.list_objects_v2(
            Bucket=e2e_config.minio_bucket_bronze,
            Prefix="klines/symbol=BTCUSDT/interval=1h/",
        )
        assert response.get("Contents"), "Expected Parquet files in bronze bucket"

    def test_silver_job_produces_output(self, e2e_config: BatchConfig) -> None:
        """After backfill, running silver should produce silver Parquet files."""
        run_silver(e2e_config)

        s3 = make_s3_client(
            endpoint=e2e_config.minio_endpoint,
            access_key=e2e_config.minio_access_key,
            secret_key=e2e_config.minio_secret_key,
        )
        response = s3.list_objects_v2(
            Bucket=e2e_config.minio_bucket_silver,
            Prefix="klines/symbol=BTCUSDT/interval=1h/",
        )
        silver_keys = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
        assert len(silver_keys) > 0, "Expected silver Parquet files after silver job"

    def test_silver_has_correct_partition_structure(
        self, e2e_config: BatchConfig
    ) -> None:
        """Silver output must have year= and month= partition directories."""
        s3 = make_s3_client(
            endpoint=e2e_config.minio_endpoint,
            access_key=e2e_config.minio_access_key,
            secret_key=e2e_config.minio_secret_key,
        )
        response = s3.list_objects_v2(
            Bucket=e2e_config.minio_bucket_silver,
            Prefix="klines/",
        )
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert any("year=" in k for k in keys)
        assert any("month=" in k for k in keys)
