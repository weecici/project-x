"""Unit tests for StreamingConfig."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from streaming.config import StreamingConfig


class TestStreamingConfigDefaults:
    """StreamingConfig fields should have sensible defaults."""

    def test_default_kafka_topics(self) -> None:
        """Default Kafka topics match the design specifications."""
        config = StreamingConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.kafka_topic_trades == "raw.trades"
        assert config.kafka_topic_klines == "raw.klines"
        assert config.kafka_topic_agg_klines == "agg.klines"
        assert config.kafka_topic_agg_vwap == "agg.vwap"

    def test_default_minio(self) -> None:
        """Default MinIO configuration points to local stack."""
        config = StreamingConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.minio_endpoint == "http://localhost:9000"
        assert config.minio_access_key == "minioadmin"
        assert config.minio_secret_key == "minioadmin"
        assert config.minio_bucket_silver == "silver"

    def test_default_spark_memory(self) -> None:
        """Default Spark memory is 1g for driver and executor."""
        config = StreamingConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.spark_driver_memory == "1g"
        assert config.spark_executor_memory == "1g"

    def test_default_streaming_thresholds(self) -> None:
        """Default thresholds are 10s watermark and 1m window."""
        config = StreamingConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.stream_watermark_delay_seconds == 10
        assert config.stream_window_duration_minutes == 1


class TestStreamingConfigValidation:
    """StreamingConfig should reject invalid field values."""

    def test_negative_watermark_raises(self) -> None:
        """A negative watermark delay must raise a ValidationError."""
        with pytest.raises(ValidationError):
            StreamingConfig(
                _env_file=None,  # type: ignore[call-arg]
                stream_watermark_delay_seconds=-5,
            )

    def test_zero_or_negative_window_raises(self) -> None:
        """A zero or negative window duration must raise a ValidationError."""
        with pytest.raises(ValidationError):
            StreamingConfig(
                _env_file=None,  # type: ignore[call-arg]
                stream_window_duration_minutes=0,
            )

        with pytest.raises(ValidationError):
            StreamingConfig(
                _env_file=None,  # type: ignore[call-arg]
                stream_window_duration_minutes=-1,
            )

    def test_custom_values_accepted(self) -> None:
        """Custom valid values are successfully parsed and stored."""
        config = StreamingConfig(
            _env_file=None,  # type: ignore[call-arg]
            stream_watermark_delay_seconds=30,
            stream_window_duration_minutes=5,
        )

        assert config.stream_watermark_delay_seconds == 30
        assert config.stream_window_duration_minutes == 5
