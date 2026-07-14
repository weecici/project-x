"""Integration tests for PySpark Structured Streaming OHLCV job.

Spins up Kafka and MinIO testcontainers, produces raw kline JSON payloads to Kafka,
runs the OHLCV streaming job, and verifies that the output klines are written
correctly to both Delta Lake (MinIO) and the aggregated Kafka topic.
"""

from __future__ import annotations

import json
import time

import pytest
from confluent_kafka import Consumer, Producer
from testcontainers.kafka import KafkaContainer
from testcontainers.minio import MinioContainer

from streaming.config import StreamingConfig
from streaming.jobs.ohlcv_stream import run_ohlcv_stream
from utils.storage import make_s3_client


@pytest.fixture(scope="module")
def kafka_container() -> KafkaContainer:
    """Start a Kafka testcontainer."""
    with KafkaContainer() as kafka:
        yield kafka


@pytest.fixture(scope="module")
def minio_container() -> MinioContainer:
    """Start a MinIO testcontainer."""
    with MinioContainer() as minio:
        yield minio


@pytest.fixture(scope="module")
def stream_config(
    kafka_container: KafkaContainer, minio_container: MinioContainer
) -> StreamingConfig:
    """Return a StreamingConfig wired to the test containers."""
    cfg = minio_container.get_config()
    return StreamingConfig(
        _env_file=None,  # type: ignore[call-arg]
        kafka_bootstrap_servers=kafka_container.get_bootstrap_server(),
        kafka_topic_klines="raw.klines.test",
        kafka_topic_agg_klines="agg.klines.test",
        minio_endpoint=f"http://{cfg['endpoint']}",
        minio_access_key=cfg["access_key"],
        minio_secret_key=cfg["secret_key"],
        minio_bucket_silver="silver-test",
        spark_driver_memory="512m",
        spark_executor_memory="512m",
        stream_watermark_delay_seconds=0,  # No delay for fast testing
        stream_window_duration_minutes=1,
        kafka_starting_offsets="earliest",
    )


@pytest.fixture(scope="module")
def _setup_infrastructure(stream_config: StreamingConfig) -> None:
    """Create the test bucket in MinIO."""
    s3 = make_s3_client(
        endpoint=stream_config.minio_endpoint,
        access_key=stream_config.minio_access_key,
        secret_key=stream_config.minio_secret_key,
    )
    s3.create_bucket(Bucket=stream_config.minio_bucket_silver)


@pytest.mark.integration
class TestStreamOHLCV:
    """Integration tests for the OHLCV streaming pipeline."""

    @pytest.mark.usefixtures("_setup_infrastructure")
    def test_ohlcv_stream_processes_and_sinks(
        self, stream_config: StreamingConfig
    ) -> None:
        """Closed kline events are correctly structured, cast, and dual-sunk."""
        # 1. Prepare mock events
        # We produce one raw kline event with is_closed=True (should be processed)
        # and one with is_closed=False (should be filtered out).
        closed_kline = {
            "event_type": "kline",
            "event_time": 1720000000000,
            "symbol": "BTCUSDT",
            "kline": {
                "open_time": 1720000000000,
                "close_time": 1720000059999,
                "symbol": "BTCUSDT",
                "interval": "1m",
                "open": "65000.00",
                "close": "65100.00",
                "high": "65200.00",
                "low": "64900.00",
                "volume": "10.5",
                "number_of_trades": 150,
                "is_closed": True,
                "quote_volume": "682500.00",
            },
        }

        open_kline = {
            "event_type": "kline",
            "event_time": 1720000010000,
            "symbol": "BTCUSDT",
            "kline": {
                "open_time": 1720000000000,
                "close_time": 1720000059999,
                "symbol": "BTCUSDT",
                "interval": "1m",
                "open": "65000.00",
                "close": "65050.00",
                "high": "65100.00",
                "low": "64900.00",
                "volume": "5.2",
                "number_of_trades": 75,
                "is_closed": False,
                "quote_volume": "338000.00",
            },
        }

        # Produce raw events to Kafka
        producer = Producer(
            {"bootstrap.servers": stream_config.kafka_bootstrap_servers}
        )
        producer.produce(
            topic=stream_config.kafka_topic_klines,
            key="BTCUSDT",
            value=json.dumps(closed_kline).encode("utf-8"),
        )
        producer.produce(
            topic=stream_config.kafka_topic_klines,
            key="BTCUSDT",
            value=json.dumps(open_kline).encode("utf-8"),
        )
        producer.flush()

        # Set up a consumer to listen on the output Kafka topic
        consumer = Consumer(
            {
                "bootstrap.servers": stream_config.kafka_bootstrap_servers,
                "group.id": "test-ohlcv-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([stream_config.kafka_topic_agg_klines])

        # 2. Run Spark Structured Streaming job
        queries = run_ohlcv_stream(stream_config)

        # Allow stream to process the micro-batch
        # Usually takes 5-8 seconds to download jars and run first trigger
        from typing import Any

        kafka_received: dict[str, Any] | None = None
        start_time = time.time()
        timeout = 25.0

        try:
            while time.time() - start_time < timeout:
                msg = consumer.poll(timeout=1.0)
                if msg is not None and not msg.error():
                    val = msg.value()
                    if val is not None:
                        kafka_received = json.loads(val.decode("utf-8"))
                        break
        finally:
            # Stop streaming queries to prevent resource leaks
            for q in queries:
                q.stop()
            # Clean up singleton SparkSession so next tests get fresh settings
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is not None:
                spark.stop()

        # 3. Assertions
        # Verify Kafka sink output (only the closed kline should be present)
        assert kafka_received is not None, (
            "Did not receive aggregated kline on Kafka topic"
        )
        assert kafka_received["symbol"] == "BTCUSDT"
        assert kafka_received["is_closed"] is True
        assert float(kafka_received["open"]) == 65000.0
        assert float(kafka_received["volume"]) == 10.5
        assert kafka_received["num_trades"] == 150

        # Verify Delta Lake sink output by listing files in MinIO
        s3 = make_s3_client(
            endpoint=stream_config.minio_endpoint,
            access_key=stream_config.minio_access_key,
            secret_key=stream_config.minio_secret_key,
        )
        objects = s3.list_objects_v2(
            Bucket=stream_config.minio_bucket_silver,
            Prefix="klines_stream/",
        )
        keys = [obj["Key"] for obj in objects.get("Contents", [])]

        # Check if Delta transaction logs and parquet files were created
        assert any("_delta_log" in k for k in keys), "Delta transaction log missing"
        assert any(k.endswith(".parquet") for k in keys), "Parquet data file missing"
