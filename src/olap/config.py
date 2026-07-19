"""Runtime configuration for the OLAP loader and BI exporter.

All settings are read from environment variables or a ``.env`` file via
pydantic-settings.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class OlapLoaderConfig(BaseSettings):
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

    # ClickHouse connection parameters
    clickhouse_host: str = Field(default="localhost")
    clickhouse_port: int = Field(default=8123, description="HTTP interface port.")
    clickhouse_db: str = Field(default="crypto")
    clickhouse_user: str = Field(default="default")
    clickhouse_password: str = Field(default="")
    clickhouse_table_klines: str = Field(
        default="klines_raw",
        description="Target ClickHouse table for kline data.",
    )

    # MinIO / S3 silver source parameters
    minio_endpoint: str = Field(default="http://localhost:9000")
    minio_access_key: str = Field(default="minioadmin")
    minio_secret_key: str = Field(default="minioadmin")
    minio_bucket_silver: str = Field(default="silver")
    silver_klines_prefix: str = Field(
        default="klines/",
        description="S3 prefix under the silver bucket for kline Parquet files.",
    )


# Alias to prevent import breakages across other components
OlapConfig = OlapLoaderConfig


class BiExporterConfig(BaseSettings):
    """Configuration for exporting metrics via Cube.js REST API."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    cube_api_url: str = Field(default="http://localhost:4000")
    cube_api_secret: str = Field(default="change_me_in_production")
    export_output_dir: Path = Field(
        default=Path(".exports"),
        description="Directory path where exported CSVs will be saved.",
    )
    google_service_account_json: str | None = Field(
        default=None,
        description="Path to Google Service Account JSON key file.",
    )
    google_sheet_name: str = Field(
        default="Crypto Platform Analytics",
        description="Name of the Google Sheets Spreadsheet to sync.",
    )
