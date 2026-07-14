"""Runtime configuration for PySpark Structured Streaming jobs.

Reads settings from environment variables or a `.env` file via pydantic-settings.
Covers Kafka topics, MinIO credentials, Spark parameters, and thresholds.
"""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StreamingConfig(BaseSettings):
    """Runtime configuration for Phase 4 Structured Streaming.

    All properties can be overridden via environment variables. The
    corresponding env-var name is the upper-cased field name
    (e.g. ``KAFKA_BOOTSTRAP_SERVERS``).
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # ------------------------------------------------------------------
    # Kafka
    # ------------------------------------------------------------------
    kafka_bootstrap_servers: str = Field(
        default="localhost:9094",
        description="Comma-separated list of Kafka bootstrap servers.",
    )
    kafka_topic_trades: str = Field(
        default="raw.trades",
        description="Topic containing raw tick-by-tick trade executions.",
    )
    kafka_topic_klines: str = Field(
        default="raw.klines",
        description="Topic containing raw kline update events.",
    )
    kafka_topic_agg_klines: str = Field(
        default="agg.klines",
        description="Downstream Kafka topic to publish finalized streaming klines.",
    )
    kafka_topic_agg_vwap: str = Field(
        default="agg.vwap",
        description=(
            "Downstream Kafka topic to publish finalized streaming "
            "VWAP/microstructure metrics."
        ),
    )
    kafka_starting_offsets: str = Field(
        default="latest",
        description="Starting offsets for Kafka streams (e.g. earliest, latest).",
    )

    # ------------------------------------------------------------------
    # MinIO / S3
    # ------------------------------------------------------------------
    minio_endpoint: str = Field(
        default="http://localhost:9000",
        description="MinIO S3-compatible service URL.",
    )
    minio_access_key: str = Field(
        default="minioadmin",
        description="MinIO root access key.",
    )
    minio_secret_key: str = Field(
        default="minioadmin",
        description="MinIO root secret key.",
    )
    minio_bucket_silver: str = Field(
        default="silver",
        description="Bucket where silver Delta tables are written.",
    )

    # ------------------------------------------------------------------
    # Spark Local Configuration
    # ------------------------------------------------------------------
    spark_driver_memory: str = Field(
        default="1g",
        description="JVM heap for the local Spark driver process.",
    )
    spark_executor_memory: str = Field(
        default="1g",
        description="JVM heap for the local Spark executor process.",
    )

    # ------------------------------------------------------------------
    # Streaming Engine Settings
    # ------------------------------------------------------------------
    stream_watermark_delay_seconds: int = Field(
        default=10,
        description="Allowed threshold (seconds) for late-arriving trade ticks.",
        ge=0,
    )
    stream_window_duration_minutes: int = Field(
        default=1,
        description="Duration (minutes) for rolling tumbling aggregation windows.",
        gt=0,
    )

    @field_validator("stream_watermark_delay_seconds")
    @classmethod
    def _validate_watermark(cls, v: int) -> int:
        """Validate watermark is non-negative.

        Args:
            v: Input watermark delay in seconds.

        Returns:
            The validated integer.

        Raises:
            ValueError: If delay is negative.
        """
        if v < 0:
            raise ValueError(f"stream_watermark_delay_seconds must be >= 0, got {v}")
        return v
