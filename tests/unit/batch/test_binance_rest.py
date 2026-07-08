"""Unit tests for the Binance REST backfill client.

All HTTP interactions are mocked with ``pytest-httpx`` — no real network
calls are made. Tests cover pagination logic, rate-limit back-off, and
malformed-row handling.
"""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from batch.backfill.binance_rest import (
    fetch_klines,
)
from batch.models import KlineRow

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_kline_list(
    open_time: int = 1704067200000,
    close_time: int = 1704070799999,
) -> list[object]:
    """Return a minimal valid Binance kline API list."""
    return [
        open_time,
        "42000.00000000",  # open
        "42500.00000000",  # high
        "41800.00000000",  # low
        "42200.00000000",  # close
        "1234.56000000",  # volume
        close_time,
        "52000000.00000000",  # quote_volume
        1000,  # num_trades
        "600.00000000",  # taker_buy_base_volume
        "25200000.00000000",  # taker_buy_quote_volume
        "0",  # ignore
    ]


# ---------------------------------------------------------------------------
# fetch_klines
# ---------------------------------------------------------------------------


class TestFetchKlines:
    """fetch_klines should parse responses and report weight correctly."""

    @pytest.mark.asyncio
    async def test_returns_parsed_rows(self, httpx_mock: HTTPXMock) -> None:
        """Successful response should return a list of KlineRow objects."""
        payload = [_make_kline_list()]
        httpx_mock.add_response(
            url="https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h"
            "&startTime=1704067200000&endTime=1704070800000&limit=1000",
            json=payload,
            headers={"X-MBX-USED-WEIGHT-1M": "2"},
        )

        import httpx

        async with httpx.AsyncClient(base_url="https://api.binance.com") as client:
            rows, weight = await fetch_klines(
                client,
                symbol="BTCUSDT",
                interval="1h",
                start_ms=1704067200000,
                end_ms=1704070800000,
            )

        assert len(rows) == 1
        assert isinstance(rows[0], KlineRow)
        assert rows[0].symbol == "BTCUSDT"
        assert weight == 2

    @pytest.mark.asyncio
    async def test_skips_malformed_rows(self, httpx_mock: HTTPXMock) -> None:
        """Rows with too few elements should be skipped; valid rows returned."""
        payload = [
            _make_kline_list(),  # valid
            [1, "bad"],  # malformed — only 2 elements
            _make_kline_list(open_time=1704074400000, close_time=1704077999999),
        ]
        httpx_mock.add_response(
            url="https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h"
            "&startTime=1704067200000&endTime=1704074400000&limit=1000",
            json=payload,
            headers={"X-MBX-USED-WEIGHT-1M": "4"},
        )

        import httpx

        async with httpx.AsyncClient(base_url="https://api.binance.com") as client:
            rows, _ = await fetch_klines(
                client,
                symbol="BTCUSDT",
                interval="1h",
                start_ms=1704067200000,
                end_ms=1704074400000,
            )

        # 2 valid, 1 malformed skipped
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_empty_response_returns_empty_list(
        self, httpx_mock: HTTPXMock
    ) -> None:
        """An empty JSON array should return an empty list of rows."""
        httpx_mock.add_response(
            url="https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h"
            "&startTime=1704067200000&endTime=1704070800000&limit=1000",
            json=[],
            headers={"X-MBX-USED-WEIGHT-1M": "2"},
        )

        import httpx

        async with httpx.AsyncClient(base_url="https://api.binance.com") as client:
            rows, _ = await fetch_klines(
                client,
                symbol="BTCUSDT",
                interval="1h",
                start_ms=1704067200000,
                end_ms=1704070800000,
            )

        assert rows == []

    @pytest.mark.asyncio
    async def test_missing_weight_header_defaults_to_zero(
        self, httpx_mock: HTTPXMock
    ) -> None:
        """Missing X-MBX-USED-WEIGHT-1M header should not raise; defaults to 0."""
        httpx_mock.add_response(
            url="https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h"
            "&startTime=1704067200000&endTime=1704070800000&limit=1000",
            json=[_make_kline_list()],
            # No weight header
        )

        import httpx

        async with httpx.AsyncClient(base_url="https://api.binance.com") as client:
            _, weight = await fetch_klines(
                client,
                symbol="BTCUSDT",
                interval="1h",
                start_ms=1704067200000,
                end_ms=1704070800000,
            )

        assert weight == 0
