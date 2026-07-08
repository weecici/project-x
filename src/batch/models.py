"""Pydantic v2 models for Binance REST API responses.

``KlineRow`` maps the Binance kline list format (12-element list per bar)
returned by ``GET /api/v3/klines`` into a validated, named Python model.
Prices are stored as ``Decimal`` to preserve the full precision of the
Binance wire format without floating-point rounding.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict


class KlineRow(BaseModel):
    """One kline (candlestick) bar from ``GET /api/v3/klines``.

    Reference:
        https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    interval: str
    open_time: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    close_time: int
    quote_volume: Decimal
    num_trades: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal

    @classmethod
    def from_api_list(
        cls,
        row: list[Any],
        *,
        symbol: str,
        interval: str,
    ) -> KlineRow:
        """Parse the raw Binance kline list format.

        Binance returns klines as 12-element lists:
        ``[open_time, open, high, low, close, volume, close_time,
           quote_volume, num_trades, taker_buy_base_vol,
           taker_buy_quote_vol, ignore]``

        Args:
            row: Raw 12-element list from the Binance API response.
            symbol: Trading-pair symbol (e.g. ``BTCUSDT``).
            interval: Kline interval string (e.g. ``1m``).

        Returns:
            A validated ``KlineRow`` instance.

        Raises:
            ValueError: If ``row`` has fewer than 11 elements.
            pydantic.ValidationError: If any field fails validation.
        """
        if len(row) < 11:
            raise ValueError(
                f"Expected at least 11 elements in kline row, got {len(row)}"
            )
        return cls(
            symbol=symbol,
            interval=interval,
            open_time=int(row[0]),
            open=Decimal(str(row[1])),
            high=Decimal(str(row[2])),
            low=Decimal(str(row[3])),
            close=Decimal(str(row[4])),
            volume=Decimal(str(row[5])),
            close_time=int(row[6]),
            quote_volume=Decimal(str(row[7])),
            num_trades=int(row[8]),
            taker_buy_base_volume=Decimal(str(row[9])),
            taker_buy_quote_volume=Decimal(str(row[10])),
        )
