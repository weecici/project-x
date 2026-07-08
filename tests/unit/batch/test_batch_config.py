"""Unit tests for BatchConfig."""

from __future__ import annotations

import pytest

from batch.config import BatchConfig


class TestBatchConfigDefaults:
    """BatchConfig fields should have sensible defaults."""

    def test_default_symbols(self) -> None:
        """Default symbols are BTCUSDT and ETHUSDT."""
        config = BatchConfig(_env_file=None)

        assert config.symbols == ["BTCUSDT", "ETHUSDT"]

    def test_default_intervals(self) -> None:
        """Default intervals cover 1m, 1h, and 1d."""
        config = BatchConfig(_env_file=None)

        assert config.kline_intervals == ["1m", "1h", "1d"]

    def test_default_start_date(self) -> None:
        """Default backfill start is 2024-01-01."""
        config = BatchConfig(_env_file=None)

        assert config.backfill_start_date == "2024-01-01"

    def test_empty_end_date_means_today(self) -> None:
        """Empty backfill_end_date is valid and left as empty string."""
        config = BatchConfig(_env_file=None)

        assert config.backfill_end_date == ""

    def test_default_spark_memory(self) -> None:
        """Default PySpark memory is 1g for both driver and executor."""
        config = BatchConfig(_env_file=None)

        assert config.spark_driver_memory == "1g"
        assert config.spark_executor_memory == "1g"


class TestBatchConfigValidation:
    """BatchConfig should reject invalid field values."""

    def test_invalid_start_date_raises(self) -> None:
        """Non ISO-8601 start date should raise ValidationError."""
        with pytest.raises(Exception, match="backfill_start_date"):
            BatchConfig(
                _env_file=None,
                backfill_start_date="not-a-date",
            )

    def test_valid_iso_date_accepted(self) -> None:
        """A valid ISO-8601 date string should be accepted."""
        config = BatchConfig(
            _env_file=None,
            backfill_start_date="2023-06-15",
        )

        assert config.backfill_start_date == "2023-06-15"

    @pytest.mark.parametrize(
        "symbol_list", [["BTCUSDT"], ["BTCUSDT", "ETHUSDT", "SOLUSDT"]]
    )
    def test_custom_symbols_accepted(self, symbol_list: list[str]) -> None:
        """Any non-empty list of symbols should be accepted."""
        config = BatchConfig(
            _env_file=None,
            symbols=symbol_list,
        )

        assert config.symbols == symbol_list
