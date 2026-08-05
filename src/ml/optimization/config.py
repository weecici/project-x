"""Configuration settings for model optimization processes in Phase 8."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class OptimizationConfig(BaseSettings):
    """Configuration class for model optimization benchmarks."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    model_name: str = Field(default="crypto_lstm")
    model_alias: str = Field(
        default="champion",
        description="MLflow registry alias pointer to retrieve target model version.",
    )
    prune_amount: float = Field(
        default=0.30,
        description="Fraction of weights to prune (0.0 to 1.0).",
        ge=0.0,
        le=1.0,
    )

    # MLflow & hardware settings
    mlflow_tracking_uri: str = Field(default="http://localhost:5000")
    device: str = Field(
        default="cpu"
    )  # CPU is default target for server-side optimization metrics

    # MinIO / Dataset settings
    minio_endpoint: str = Field(default="http://localhost:9000")
    minio_access_key: str = Field(default="minioadmin")
    minio_secret_key: str = Field(default="minioadmin")
    minio_bucket_gold: str = Field(default="gold")
    seq_length: int = Field(default=60)
    output_dir: Path = Field(default=Path(".exports"))
