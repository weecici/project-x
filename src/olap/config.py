"""Runtime configuration for the OLAP loading pipeline.

All settings are read from environment variables or a ``.env`` file via
pydantic-settings. Covers ClickHouse connection and MinIO silver source.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class OlapConfig(BaseSettings):
    """Runtime configuration for the silver → ClickHouse loader.

    All fields can be overridden via environment variables using their
    upper-cased field name (e.g. ``CLICKHOUSE_HOST``).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # ClickHouse
    # ------------------------------------------------------------------
    clickhouse_host: str = Field(default="localhost")
    clickhouse_port: int = Field(default=8123, description="HTTP interface port.")
    clickhouse_db: str = Field(default="silver")
    clickhouse_user: str = Field(default="default")
    clickhouse_password: str = Field(default="")
    clickhouse_table_klines: str = Field(
        default="klines_raw",
        description="Target ClickHouse table for kline data.",
    )

    # ------------------------------------------------------------------
    # MinIO silver source
    # ------------------------------------------------------------------
    minio_endpoint: str = Field(default="http://localhost:9000")
    minio_access_key: str = Field(default="minioadmin")
    minio_secret_key: str = Field(default="minioadmin")
    minio_bucket_silver: str = Field(default="silver")
    silver_klines_prefix: str = Field(
        default="klines/",
        description="S3 prefix under the silver bucket for kline Parquet files.",
    )
