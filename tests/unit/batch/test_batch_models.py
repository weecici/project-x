"""Unit tests for KlineRow model parsing."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from batch.models import KlineRow

# A realistic Binance kline API list response for one bar.
_SAMPLE_ROW: list[object] = [
    1704067200000,  # open_time  (2024-01-01 00:00:00 UTC)
    "42000.50000000",  # open
    "42500.00000000",  # high
    "41800.00000000",  # low
    "42200.00000000",  # close
    "1234.56789000",  # volume
    1704070799999,  # close_time
    "52123456.78900000",  # quote_volume
    5432,  # num_trades
    "617.28394500",  # taker_buy_base_volume
    "26061728.39450000",  # taker_buy_quote_volume
    "0",  # ignore field
]


class TestKlineRowFromApiList:
    """KlineRow.from_api_list should parse Binance list format correctly."""

    def test_parses_all_fields(self) -> None:
        """All 11 meaningful fields should be parsed correctly."""
        row = KlineRow.from_api_list(_SAMPLE_ROW, symbol="BTCUSDT", interval="1h")

        assert row.symbol == "BTCUSDT"
        assert row.interval == "1h"
        assert row.open_time == 1704067200000
        assert row.open == Decimal("42000.50000000")
        assert row.high == Decimal("42500.00000000")
        assert row.low == Decimal("41800.00000000")
        assert row.close == Decimal("42200.00000000")
        assert row.volume == Decimal("1234.56789000")
        assert row.close_time == 1704070799999
        assert row.num_trades == 5432

    def test_decimal_precision_preserved(self) -> None:
        """Decimal fields must not lose precision compared to the wire format."""
        row = KlineRow.from_api_list(_SAMPLE_ROW, symbol="BTCUSDT", interval="1h")

        assert str(row.open) == "42000.50000000"
        assert str(row.taker_buy_base_volume) == "617.28394500"

    def test_raises_on_too_short_list(self) -> None:
        """A list with fewer than 11 elements should raise ValueError."""
        with pytest.raises(ValueError, match="at least 11 elements"):
            KlineRow.from_api_list([1, "2", "3"], symbol="BTCUSDT", interval="1h")

    @pytest.mark.parametrize(
        "symbol, interval",
        [("BTCUSDT", "1m"), ("ETHUSDT", "1h"), ("SOLUSDT", "1d")],
    )
    def test_symbol_and_interval_injected(self, symbol: str, interval: str) -> None:
        """symbol and interval from kwargs should appear on the model."""
        row = KlineRow.from_api_list(_SAMPLE_ROW, symbol=symbol, interval=interval)

        assert row.symbol == symbol
        assert row.interval == interval

    def test_immutable_model(self) -> None:
        """KlineRow is frozen — mutating a field should raise."""
        row = KlineRow.from_api_list(_SAMPLE_ROW, symbol="BTCUSDT", interval="1h")

        with pytest.raises(ValidationError):
            row.open = Decimal("0")
