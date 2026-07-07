"""Centralised configuration for the ingestion pipeline.

All settings are read from environment variables or a `.env` file
(via pydantic-settings). No defaults contain sensitive values.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionConfig(BaseSettings):
    """Runtime configuration for Binance WS producer and lake writer.

    All fields can be overridden via environment variables. The
    corresponding env-var name is the upper-cased field name
    (e.g. ``KAFKA_BOOTSTRAP_SERVERS``).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ------------------------------------------------------------------
    # Kafka
    # ------------------------------------------------------------------
    kafka_bootstrap_servers: str = Field(
        default="localhost:9094",
        description="Comma-separated Kafka bootstrap servers (host access).",
    )
    kafka_topic_trades: str = Field(default="raw.trades")
    kafka_topic_klines: str = Field(default="raw.klines")
    kafka_dlq_trades: str = Field(
        default="raw.trades.dlq",
        description="Dead-letter topic for trades that fail validation.",
    )
    kafka_dlq_klines: str = Field(
        default="raw.klines.dlq",
        description="Dead-letter topic for klines that fail validation.",
    )

    # ------------------------------------------------------------------
    # MinIO / S3
    # ------------------------------------------------------------------
    minio_endpoint: str = Field(default="http://localhost:9000")
    minio_access_key: str = Field(default="minioadmin")
    minio_secret_key: str = Field(default="minioadmin")
    minio_bucket_bronze: str = Field(default="bronze")

    # ------------------------------------------------------------------
    # Binance WebSocket
    # ------------------------------------------------------------------
    binance_ws_base_url: str = Field(
        default="wss://stream.binance.com:9443",
        description="Binance combined-stream WebSocket base URL.",
    )
    symbols: list[str] = Field(
        default=["BTCUSDT", "ETHUSDT"],
        description="List of trading-pair symbols to subscribe to.",
    )
    kline_intervals: list[str] = Field(
        default=["1m"],
        description="Kline intervals to subscribe to (e.g. 1m, 5m, 1h).",
    )

    # ------------------------------------------------------------------
    # Lake writer flush thresholds
    # ------------------------------------------------------------------
    lake_flush_rows: int = Field(
        default=1_000,
        description="Flush to MinIO after this many buffered rows.",
        gt=0,
    )
    lake_flush_seconds: int = Field(
        default=30,
        description="Flush to MinIO after this many seconds regardless of row count.",
        gt=0,
    )
