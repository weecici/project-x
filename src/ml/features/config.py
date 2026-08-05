"""Configuration settings for feature engineering in Phase 8.

All configuration values can be overridden via environment variables
prefixed with standard naming conventions.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class FeatureConfig(BaseSettings):
    """Configuration class for ML feature engineering."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    symbols: list[str] = Field(
        default=["BTCUSDT", "ETHUSDT"],
        description="List of trading-pair symbols to compute features for.",
    )
    seq_length: int = Field(
        default=60,
        description="Sequence lookback window length.",
        gt=0,
    )
    target_horizon: int = Field(
        default=1,
        description="Prediction target horizon in terms of kline steps.",
        gt=0,
    )

    # Storage paths
    minio_endpoint: str = Field(default="http://localhost:9000")
    minio_access_key: str = Field(default="minioadmin")
    minio_secret_key: str = Field(default="minioadmin")
    minio_bucket_silver: str = Field(default="silver")
    minio_bucket_gold: str = Field(default="gold")

    # Spark execution
    spark_master: str = Field(
        default="local[*]",
        description="Spark master URL for feature execution.",
    )

    # MLflow settings
    mlflow_tracking_uri: str = Field(default="http://localhost:5000")
    mlflow_experiment_name: str = Field(default="crypto_price_direction")
