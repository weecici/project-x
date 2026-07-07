"""Pydantic v2 models for Binance WebSocket event types.

Each model validates and normalises the raw Binance JSON payload
(which uses single-character alias keys) into a readable Python schema.
All field aliases map exactly to the Binance API wire format.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class TradeEvent(BaseModel):
    """A single trade execution event from the Binance trade stream.

    Reference:
        https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#trade-streams
    """

    model_config = ConfigDict(populate_by_name=True)

    event_type: str = Field(alias="e", description="Event type identifier ('trade').")
    event_time: int = Field(alias="E", description="Event timestamp in milliseconds.")
    symbol: str = Field(alias="s", description="Trading pair symbol (e.g. BTCUSDT).")
    trade_id: int = Field(alias="t", description="Unique trade identifier.")
    price: str = Field(alias="p", description="Trade execution price (string decimal).")
    quantity: str = Field(alias="q", description="Trade quantity (string decimal).")
    buyer_order_id: int | None = Field(
        default=None,
        alias="b",
        description="Buyer order ID.",
    )
    seller_order_id: int | None = Field(
        default=None,
        alias="a",
        description="Seller order ID.",
    )
    trade_time: int = Field(alias="T", description="Trade timestamp in milliseconds.")
    is_buyer_maker: bool = Field(
        alias="m",
        description="True if the buyer is the market maker.",
    )


class KlineData(BaseModel):
    """Nested kline (candlestick) bar data within a KlineEvent.

    Prices and volumes are kept as strings to preserve the exact
    decimal representation sent by Binance.
    """

    model_config = ConfigDict(populate_by_name=True)

    open_time: int = Field(alias="t", description="Kline open time in milliseconds.")
    close_time: int = Field(alias="T", description="Kline close time in milliseconds.")
    symbol: str = Field(alias="s")
    interval: str = Field(alias="i", description="Kline interval (e.g. '1m').")
    open: str = Field(alias="o", description="Open price.")
    close: str = Field(alias="c", description="Close price.")
    high: str = Field(alias="h", description="High price.")
    low: str = Field(alias="l", description="Low price.")
    volume: str = Field(alias="v", description="Base asset volume.")
    number_of_trades: int = Field(alias="n", description="Number of trades in bar.")
    is_closed: bool = Field(
        alias="x",
        description="True if this kline bar is closed (final values).",
    )
    quote_volume: str = Field(alias="q", description="Quote asset volume.")


class KlineEvent(BaseModel):
    """A kline/candlestick update event from the Binance kline stream."""

    model_config = ConfigDict(populate_by_name=True)

    event_type: str = Field(alias="e")
    event_time: int = Field(alias="E")
    symbol: str = Field(alias="s")
    kline: KlineData = Field(alias="k")
