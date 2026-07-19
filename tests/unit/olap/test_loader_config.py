"""Unit tests for OlapLoaderConfig."""

from __future__ import annotations

import pytest

from olap.config import OlapLoaderConfig


class TestOlapLoaderConfigDefaults:
    """OlapLoaderConfig should have correct defaults for local dev."""

    def test_clickhouse_defaults(self) -> None:
        """Default ClickHouse connection points to localhost:8123."""
        config = OlapLoaderConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.clickhouse_host == "localhost"
        assert config.clickhouse_port == 8123
        assert config.clickhouse_db == "crypto"
        assert config.clickhouse_user == "default"
        assert config.clickhouse_password == ""

    def test_default_table(self) -> None:
        """Default target table is klines_raw."""
        config = OlapLoaderConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.clickhouse_table_klines == "klines_raw"

    def test_minio_defaults(self) -> None:
        """Default MinIO connection points to localhost:9000."""
        config = OlapLoaderConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.minio_endpoint == "http://localhost:9000"
        assert config.minio_bucket_silver == "silver"
        assert config.silver_klines_prefix == "klines/"


class TestOlapLoaderConfigEnvOverrides:
    """OlapLoaderConfig fields should be overridable via env vars."""

    def test_clickhouse_host_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CLICKHOUSE_HOST env var should override the default."""
        monkeypatch.setenv("CLICKHOUSE_HOST", "clickhouse-server")
        config = OlapLoaderConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.clickhouse_host == "clickhouse-server"

    def test_clickhouse_db_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CLICKHOUSE_DB env var should override the default."""
        monkeypatch.setenv("CLICKHOUSE_DB", "analytics")
        config = OlapLoaderConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.clickhouse_db == "analytics"

    def test_silver_prefix_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """SILVER_KLINES_PREFIX env var should override the default."""
        monkeypatch.setenv("SILVER_KLINES_PREFIX", "klines/v2/")
        config = OlapLoaderConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.silver_klines_prefix == "klines/v2/"
