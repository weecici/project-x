"""Runtime configuration for the batch processing pipeline.

All settings are read from environment variables or a ``.env`` file via
pydantic-settings. Covers Binance REST credentials, MinIO bucket names,
backfill date range, and PySpark tuning knobs.
"""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class BatchConfig(BaseSettings):
    """Runtime configuration for batch backfill and PySpark silver jobs.

    All fields can be overridden via environment variables. The
    corresponding env-var name is the upper-cased field name
    (e.g. ``BACKFILL_START_DATE``).
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # ------------------------------------------------------------------
    # Binance REST API
    # ------------------------------------------------------------------
    binance_rest_base_url: str = Field(
        default="https://api.binance.com",
        description="Base URL for Binance Spot REST API.",
    )
    binance_api_key: str = Field(
        default="",
        description=(
            "Optional Binance API key. Omit for public endpoints. "
            "A key raises the rate-limit weight from 1 200 to 6 000/min."
        ),
    )
    symbols: list[str] = Field(
        default=["BTCUSDT", "ETHUSDT"],
        description="Trading-pair symbols to backfill.",
    )
    kline_intervals: list[str] = Field(
        default=["1m", "1h", "1d"],
        description=(
            "Kline intervals to backfill. Add '5m' or '15m' here if needed; "
            "no code changes required."
        ),
    )
    backfill_start_date: str = Field(
        default="2024-01-01",
        description="ISO-8601 date (YYYY-MM-DD); start of the historical fetch window.",
    )
    backfill_end_date: str = Field(
        default="",
        description=(
            "ISO-8601 date (YYYY-MM-DD); end of the historical fetch window. "
            "Empty string defaults to today (UTC)."
        ),
    )

    # ------------------------------------------------------------------
    # MinIO / S3
    # ------------------------------------------------------------------
    minio_endpoint: str = Field(default="http://localhost:9000")
    minio_access_key: str = Field(default="minioadmin")
    minio_secret_key: str = Field(default="minioadmin")
    minio_bucket_bronze: str = Field(default="bronze")
    minio_bucket_silver: str = Field(default="silver")

    # ------------------------------------------------------------------
    # PySpark (local[*] standalone — no cluster)
    # ------------------------------------------------------------------
    spark_driver_memory: str = Field(
        default="1g",
        description="JVM heap for the Spark driver process.",
    )
    spark_executor_memory: str = Field(
        default="1g",
        description="JVM heap for the Spark executor process.",
    )

    @field_validator("backfill_start_date")
    @classmethod
    def _validate_start_date(cls, v: str) -> str:
        """Raise ValueError if start_date is not a valid ISO-8601 date.

        Args:
            v: Raw date string from config.

        Returns:
            The validated date string unchanged.

        Raises:
            ValueError: If the string cannot be parsed as YYYY-MM-DD.
        """
        from datetime import date

        try:
            date.fromisoformat(v)
        except ValueError as exc:
            raise ValueError(
                f"backfill_start_date must be YYYY-MM-DD, got {v!r}"
            ) from exc
        return v
