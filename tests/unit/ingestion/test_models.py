"""Unit tests for ingestion Pydantic models.

Tests focus on field alias mapping, validation correctness, and
round-trip serialisation. No I/O or external services required.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ingestion.models import KlineEvent, TradeEvent

# ---------------------------------------------------------------------------
# Test fixtures (shared payloads matching the Binance wire format)
# ---------------------------------------------------------------------------

VALID_TRADE_PAYLOAD: dict[str, object] = {
    "e": "trade",
    "E": 1_720_000_000_000,
    "s": "BTCUSDT",
    "t": 123_456,
    "p": "65000.00",
    "q": "0.001",
    "b": 111,
    "a": 222,
    "T": 1_720_000_000_000,
    "m": False,
}

VALID_KLINE_PAYLOAD: dict[str, object] = {
    "e": "kline",
    "E": 1_720_000_000_000,
    "s": "BTCUSDT",
    "k": {
        "t": 1_720_000_000_000,
        "T": 1_720_000_059_999,
        "s": "BTCUSDT",
        "i": "1m",
        "o": "65000.00",
        "c": "65100.00",
        "h": "65200.00",
        "l": "64900.00",
        "v": "10.5",
        "n": 150,
        "x": False,
        "q": "682500.00",
    },
}


# ---------------------------------------------------------------------------
# TradeEvent
# ---------------------------------------------------------------------------


class TestTradeEvent:
    """Tests for the TradeEvent Pydantic model."""

    def test_validates_correct_payload(self) -> None:
        """Parse a valid trade payload without raising."""
        # Arrange / Act
        event = TradeEvent.model_validate(VALID_TRADE_PAYLOAD)

        # Assert
        assert event.symbol == "BTCUSDT"
        assert event.price == "65000.00"
        assert event.quantity == "0.001"
        assert event.is_buyer_maker is False

    def test_alias_mapping_to_python_names(self) -> None:
        """Single-char Binance aliases map to readable Python attribute names."""
        event = TradeEvent.model_validate(VALID_TRADE_PAYLOAD)

        assert event.event_type == "trade"
        assert event.event_time == 1_720_000_000_000
        assert event.trade_id == 123_456

    def test_missing_required_field_raises_validation_error(self) -> None:
        """ValidationError is raised when a required field is absent."""
        # Arrange: drop symbol field ("s")
        incomplete = {k: v for k, v in VALID_TRADE_PAYLOAD.items() if k != "s"}

        # Act / Assert
        with pytest.raises(ValidationError):
            TradeEvent.model_validate(incomplete)

    def test_json_round_trip_preserves_all_values(self) -> None:
        """model_dump → model_validate round-trip produces an equal model."""
        # Arrange
        original = TradeEvent.model_validate(VALID_TRADE_PAYLOAD)

        # Act
        dumped = original.model_dump(mode="json")
        restored = TradeEvent.model_validate(dumped)

        # Assert
        assert original == restored

    @pytest.mark.parametrize(
        "field_alias",
        ["p", "q", "t"],
        ids=["price", "quantity", "trade_id"],
    )
    def test_missing_core_fields_raise(self, field_alias: str) -> None:
        """ValidationError is raised for each missing core trade field."""
        # Arrange
        incomplete = {k: v for k, v in VALID_TRADE_PAYLOAD.items() if k != field_alias}

        # Act / Assert
        with pytest.raises(ValidationError):
            TradeEvent.model_validate(incomplete)


# ---------------------------------------------------------------------------
# KlineEvent / KlineData
# ---------------------------------------------------------------------------


class TestKlineEvent:
    """Tests for the KlineEvent and nested KlineData models."""

    def test_validates_correct_payload(self) -> None:
        """Parse a valid kline payload without raising."""
        event = KlineEvent.model_validate(VALID_KLINE_PAYLOAD)

        assert event.symbol == "BTCUSDT"
        assert event.event_type == "kline"

    def test_nested_kline_data_parses_ohlcv(self) -> None:
        """Nested KlineData OHLCV fields all parse correctly."""
        # Arrange / Act
        event = KlineEvent.model_validate(VALID_KLINE_PAYLOAD)
        kline = event.kline

        # Assert
        assert kline.interval == "1m"
        assert kline.open == "65000.00"
        assert kline.close == "65100.00"
        assert kline.high == "65200.00"
        assert kline.low == "64900.00"
        assert kline.volume == "10.5"
        assert kline.number_of_trades == 150
        assert kline.is_closed is False

    @pytest.mark.parametrize(
        "missing_alias",
        ["o", "c", "h", "l", "v"],
        ids=["open", "close", "high", "low", "volume"],
    )
    def test_missing_ohlcv_field_raises(self, missing_alias: str) -> None:
        """ValidationError is raised when any OHLCV field is missing."""
        # Arrange
        k_val = VALID_KLINE_PAYLOAD["k"]
        assert isinstance(k_val, dict)
        bad_kline = {k: v for k, v in k_val.items() if k != missing_alias}
        bad_payload = {**VALID_KLINE_PAYLOAD, "k": bad_kline}

        # Act / Assert
        with pytest.raises(ValidationError):
            KlineEvent.model_validate(bad_payload)

    def test_json_round_trip_preserves_all_values(self) -> None:
        """model_dump → model_validate round-trip produces an equal model."""
        original = KlineEvent.model_validate(VALID_KLINE_PAYLOAD)
        dumped = original.model_dump(mode="json")
        restored = KlineEvent.model_validate(dumped)

        assert original == restored
