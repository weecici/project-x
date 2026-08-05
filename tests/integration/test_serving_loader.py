"""Integration tests for the serving ClickHouse loader.

Uses ``testcontainers`` to spin up ClickHouse and MinIO.
"""

from __future__ import annotations

import io
from decimal import Decimal

import clickhouse_connect
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.minio import MinioContainer

from olap.config import OlapLoaderConfig
from olap.loader import load_klines
from utils.storage import make_s3_client

# Fixture rows — two bars for BTCUSDT/1h
_FIXTURE_ROWS = [
    {
        "symbol": "BTCUSDT",
        "interval": "1h",
        "open_time": 1704067200000,  # 2024-01-01 00:00 UTC
        "open": Decimal("42000.00000000"),
        "high": Decimal("42500.00000000"),
        "low": Decimal("41800.00000000"),
        "close": Decimal("42200.00000000"),
        "volume": Decimal("100.00000000"),
        "close_time": 1704070799999,
        "quote_volume": Decimal("4200000.00000000"),
        "num_trades": 1000,
        "taker_buy_base_volume": Decimal("50.00000000"),
        "taker_buy_quote_volume": Decimal("2100000.00000000"),
    },
    {
        "symbol": "BTCUSDT",
        "interval": "1h",
        "open_time": 1704070800000,  # 2024-01-01 01:00 UTC
        "open": Decimal("42200.00000000"),
        "high": Decimal("42800.00000000"),
        "low": Decimal("42100.00000000"),
        "close": Decimal("42600.00000000"),
        "volume": Decimal("200.00000000"),
        "close_time": 1704074399999,
        "quote_volume": Decimal("8400000.00000000"),
        "num_trades": 2000,
        "taker_buy_base_volume": Decimal("100.00000000"),
        "taker_buy_quote_volume": Decimal("4200000.00000000"),
    },
]


@pytest.fixture(scope="module")
def clickhouse_container() -> ClickHouseContainer:
    """Start a ClickHouse testcontainer for the module."""
    with ClickHouseContainer("clickhouse/clickhouse-server:24.3-alpine") as ch:
        yield ch


@pytest.fixture(scope="module")
def minio_container() -> MinioContainer:
    """Start a MinIO testcontainer for the module."""
    with MinioContainer() as minio:
        yield minio


@pytest.fixture(scope="module")
def loader_config(
    clickhouse_container: ClickHouseContainer,
    minio_container: MinioContainer,
) -> OlapLoaderConfig:
    """Return OlapLoaderConfig wired to the testcontainers."""
    ch_host = clickhouse_container.get_container_host_ip()
    ch_port = int(clickhouse_container.get_exposed_port(8123))
    minio_cfg = minio_container.get_config()

    return OlapLoaderConfig(
        _env_file=None,  # type: ignore[call-arg]
        clickhouse_host=ch_host,
        clickhouse_port=ch_port,
        clickhouse_db="silver",
        clickhouse_user=clickhouse_container.username,
        clickhouse_password=clickhouse_container.password,
        minio_endpoint=f"http://{minio_cfg['endpoint']}",
        minio_access_key=minio_cfg["access_key"],
        minio_secret_key=minio_cfg["secret_key"],
        minio_bucket_silver="silver",
        silver_klines_prefix="klines/",
    )


@pytest.fixture(scope="module")
def _upload_fixture_parquet(loader_config: OlapLoaderConfig) -> None:
    """Create silver bucket and upload two-row fixture Parquet."""
    s3 = make_s3_client(
        endpoint=loader_config.minio_endpoint,
        access_key=loader_config.minio_access_key,
        secret_key=loader_config.minio_secret_key,
    )
    s3.create_bucket(Bucket=loader_config.minio_bucket_silver)

    table = pa.Table.from_pylist(_FIXTURE_ROWS)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(
        Bucket=loader_config.minio_bucket_silver,
        Key="klines/symbol=BTCUSDT/interval=1h/year=2024/month=01/fixture.parquet",
        Body=buf.getvalue(),
    )


@pytest.mark.integration
class TestServingLoader:
    """Integration tests for the silver → ClickHouse loader."""

    def test_load_klines_returns_correct_row_count(
        self, loader_config: OlapLoaderConfig, _upload_fixture_parquet: None
    ) -> None:
        """load_klines should return total rows inserted (2)."""
        total = load_klines(loader_config)

        assert total == 2

    def test_rows_queryable_from_clickhouse(
        self, loader_config: OlapLoaderConfig, _upload_fixture_parquet: None
    ) -> None:
        """Inserted rows should be queryable from ClickHouse."""
        client = clickhouse_connect.get_client(
            host=loader_config.clickhouse_host,
            port=loader_config.clickhouse_port,
            database=loader_config.clickhouse_db,
            username=loader_config.clickhouse_user,
            password=loader_config.clickhouse_password,
        )
        result = client.query(
            "SELECT count() FROM silver.klines_raw FINAL WHERE symbol = 'BTCUSDT'"
        )
        count = result.result_rows[0][0]

        assert count == 2

    def test_second_load_is_idempotent(
        self, loader_config: OlapLoaderConfig, _upload_fixture_parquet: None
    ) -> None:
        """Re-running load_klines should not duplicate rows."""
        load_klines(loader_config)

        client = clickhouse_connect.get_client(
            host=loader_config.clickhouse_host,
            port=loader_config.clickhouse_port,
            database=loader_config.clickhouse_db,
            username=loader_config.clickhouse_user,
            password=loader_config.clickhouse_password,
        )
        result = client.query(
            "SELECT count() FROM silver.klines_raw FINAL WHERE symbol = 'BTCUSDT'"
        )
        count = result.result_rows[0][0]

        assert count == 2
