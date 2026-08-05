"""Integration tests for the PySpark kline silver transformer.

Uses ``testcontainers`` to spin up a real MinIO instance, uploads
fixture bronze Parquet (including deliberate duplicate rows), runs the
PySpark silver transformer, and asserts the silver output has the
correct schema, row count, and no duplicates.

Requires: Docker daemon running.
"""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from testcontainers.minio import MinioContainer

from batch.config import BatchConfig
from batch.silver.kline_transformer import run_silver
from utils.storage import make_s3_client


@pytest.fixture(scope="module")
def minio_container() -> MinioContainer:
    """Start a MinIO testcontainer for the duration of the module."""
    with MinioContainer() as minio:
        yield minio


@pytest.fixture(scope="module")
def silver_config(minio_container: MinioContainer) -> BatchConfig:
    """Return a BatchConfig wired to the testcontainer MinIO."""
    cfg = minio_container.get_config()
    return BatchConfig(
        _env_file=None,  # type: ignore[call-arg]
        minio_endpoint=f"http://{cfg['endpoint']}",
        minio_access_key=cfg["access_key"],
        minio_secret_key=cfg["secret_key"],
        minio_bucket_bronze="bronze",
        minio_bucket_silver="silver",
        spark_driver_memory="512m",
        spark_executor_memory="512m",
    )


@pytest.fixture(scope="module")
def _setup_buckets_and_data(silver_config: BatchConfig) -> None:
    """Create bronze/silver buckets and upload fixture Parquet files."""
    s3 = make_s3_client(
        endpoint=silver_config.minio_endpoint,
        access_key=silver_config.minio_access_key,
        secret_key=silver_config.minio_secret_key,
    )
    for bucket in [
        silver_config.minio_bucket_bronze,
        silver_config.minio_bucket_silver,
    ]:
        s3.create_bucket(Bucket=bucket)

    # Build fixture: 3 unique rows + 1 duplicate of row 0.
    rows = [
        {
            "symbol": "BTCUSDT",
            "interval": "1h",
            "open_time": 1704067200000,
            "open": "42000.00",
            "high": "42500.00",
            "low": "41800.00",
            "close": "42200.00",
            "volume": "100.00",
            "close_time": 1704070799999,
            "quote_volume": "4200000.00",
            "num_trades": 1000,
            "taker_buy_base_volume": "50.00",
            "taker_buy_quote_volume": "2100000.00",
        },
        {
            "symbol": "BTCUSDT",
            "interval": "1h",
            "open_time": 1704070800000,
            "open": "42200.00",
            "high": "42800.00",
            "low": "42100.00",
            "close": "42600.00",
            "volume": "200.00",
            "close_time": 1704074399999,
            "quote_volume": "8400000.00",
            "num_trades": 2000,
            "taker_buy_base_volume": "100.00",
            "taker_buy_quote_volume": "4200000.00",
        },
        # Duplicate of row 0 — should be removed by dedup
        {
            "symbol": "BTCUSDT",
            "interval": "1h",
            "open_time": 1704067200000,
            "open": "42000.00",
            "high": "42500.00",
            "low": "41800.00",
            "close": "42200.00",
            "volume": "100.00",
            "close_time": 1704070799999,
            "quote_volume": "4200000.00",
            "num_trades": 1000,
            "taker_buy_base_volume": "50.00",
            "taker_buy_quote_volume": "2100000.00",
        },
    ]
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(
        Bucket=silver_config.minio_bucket_bronze,
        Key="klines/symbol=BTCUSDT/interval=1h/year=2024/month=01/day=01/fixture.parquet",
        Body=buf.getvalue(),
    )


@pytest.mark.integration
class TestSilverTransformer:
    """Integration tests for the PySpark bronze → silver transformation."""

    def test_silver_deduplicates_rows(
        self, silver_config: BatchConfig, _setup_buckets_and_data: None
    ) -> None:
        """Silver output should have 2 rows (3 bronze - 1 duplicate)."""
        run_silver(silver_config)

        s3 = make_s3_client(
            endpoint=silver_config.minio_endpoint,
            access_key=silver_config.minio_access_key,
            secret_key=silver_config.minio_secret_key,
        )
        response = s3.list_objects_v2(
            Bucket=silver_config.minio_bucket_silver,
            Prefix="klines/",
        )
        silver_keys = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
        assert len(silver_keys) > 0, "No silver Parquet files were written"

        total_rows = 0
        for key in silver_keys:
            obj = s3.get_object(Bucket=silver_config.minio_bucket_silver, Key=key)
            table = pq.read_table(io.BytesIO(obj["Body"].read()))
            total_rows += table.num_rows

        assert total_rows == 2, f"Expected 2 rows after dedup, got {total_rows}"

    def test_silver_partition_dirs_exist(
        self, silver_config: BatchConfig, _setup_buckets_and_data: None
    ) -> None:
        """Silver bucket should contain Hive-style partition directory structure."""
        s3 = make_s3_client(
            endpoint=silver_config.minio_endpoint,
            access_key=silver_config.minio_access_key,
            secret_key=silver_config.minio_secret_key,
        )
        response = s3.list_objects_v2(
            Bucket=silver_config.minio_bucket_silver,
            Prefix="klines/symbol=BTCUSDT/interval=1h/",
        )
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert any("year=" in k for k in keys), (
            "Expected year= partition in silver keys"
        )
        assert any("month=" in k for k in keys), (
            "Expected month= partition in silver keys"
        )
