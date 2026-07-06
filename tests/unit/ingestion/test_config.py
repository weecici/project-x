"""Unit tests for IngestionConfig settings.

Validates default values, type coercion, and environment-variable
overrides. No I/O or external services required.
"""

from __future__ import annotations

import pytest

from ingestion.config import IngestionConfig


class TestIngestionConfigDefaults:
    """Tests for IngestionConfig default values."""

    def test_kafka_topic_defaults(self) -> None:
        """Default Kafka topic names match the architecture spec."""
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.kafka_topic_trades == "raw.trades"
        assert config.kafka_topic_klines == "raw.klines"

    def test_dlq_topic_defaults(self) -> None:
        """Dead-letter queue topic names are correctly defaulted."""
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.kafka_dlq_trades == "raw.trades.dlq"
        assert config.kafka_dlq_klines == "raw.klines.dlq"

    def test_minio_bucket_defaults(self) -> None:
        """Default MinIO bucket is 'bronze'."""
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.minio_bucket_bronze == "bronze"

    def test_default_symbols_include_btc_and_eth(self) -> None:
        """Default symbols list includes at least BTCUSDT and ETHUSDT."""
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        assert "BTCUSDT" in config.symbols
        assert "ETHUSDT" in config.symbols

    def test_default_kline_interval_is_one_minute(self) -> None:
        """Default kline interval is '1m'."""
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        assert "1m" in config.kline_intervals

    def test_flush_thresholds_are_positive(self) -> None:
        """Default flush thresholds are positive integers."""
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.lake_flush_rows > 0
        assert config.lake_flush_seconds > 0


class TestIngestionConfigEnvOverrides:
    """Tests for environment-variable overrides on IngestionConfig."""

    def test_override_symbols_via_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """SYMBOLS env var replaces the default symbol list."""
        # Arrange
        monkeypatch.setenv("SYMBOLS", '["SOLUSDT"]')

        # Act
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        # Assert
        assert config.symbols == ["SOLUSDT"]

    def test_override_flush_rows_via_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """LAKE_FLUSH_ROWS env var overrides the default row threshold."""
        # Arrange
        monkeypatch.setenv("LAKE_FLUSH_ROWS", "250")

        # Act
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        # Assert
        assert config.lake_flush_rows == 250

    def test_override_kafka_servers_via_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """KAFKA_BOOTSTRAP_SERVERS env var overrides the broker address."""
        # Arrange
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

        # Act
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        # Assert
        assert config.kafka_bootstrap_servers == "kafka:9092"

    def test_override_minio_endpoint_via_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """MINIO_ENDPOINT env var overrides the default endpoint URL."""
        # Arrange
        monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")

        # Act
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        # Assert
        assert config.minio_endpoint == "http://minio:9000"

    @pytest.mark.parametrize(
        "flush_rows",
        [1, 100, 10_000],
        ids=["min", "typical", "large"],
    )
    def test_various_flush_row_values_accepted(
        self, flush_rows: int, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Any positive LAKE_FLUSH_ROWS value is accepted."""
        # Arrange
        monkeypatch.setenv("LAKE_FLUSH_ROWS", str(flush_rows))

        # Act
        config = IngestionConfig(_env_file=None)  # type: ignore[call-arg]

        # Assert
        assert config.lake_flush_rows == flush_rows
