"""Integration tests for PySpark Structured Streaming VWAP job.

Spins up Kafka and MinIO testcontainers, produces trade events (including duplicates
and out-of-order events) to Kafka, executes the VWAP streaming job, and verifies that:
1. Duplicate trades are correctly filtered out.
2. Out-of-order trades within the watermark are correctly aggregated.
3. VWAP, OFI, price volatility, and trade counts are calculated accurately.
4. Outputs are dual-sunk to Delta Lake and the agg.vwap Kafka topic.
"""

from __future__ import annotations

import json
import time

import pytest
from confluent_kafka import Consumer, Producer
from testcontainers.kafka import KafkaContainer
from testcontainers.minio import MinioContainer

from streaming.config import StreamingConfig
from streaming.jobs.vwap_stream import run_vwap_stream
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
        kafka_topic_trades="raw.trades.test",
        kafka_topic_agg_vwap="agg.vwap.test",
        minio_endpoint=f"http://{cfg['endpoint']}",
        minio_access_key=cfg["access_key"],
        minio_secret_key=cfg["secret_key"],
        minio_bucket_silver="silver-test-vwap",
        spark_driver_memory="512m",
        spark_executor_memory="512m",
        stream_watermark_delay_seconds=10,  # 10s watermark for out-of-order testing
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
class TestStreamVWAP:
    """Integration tests for the VWAP and microstructure metrics pipeline."""

    @pytest.mark.usefixtures("_setup_infrastructure")
    def test_vwap_stream_computes_microstructure_metrics(
        self, stream_config: StreamingConfig
    ) -> None:
        """Windowed VWAP, OFI, volatility, and trade count are calculated correctly."""
        # 1. Prepare mock execution events
        # Baseline window is 10:00:00 - 10:01:00 UTC (unix timestamps in ms)
        trades = [
            # Trade 1: Taker buy (positive OFI)
            {
                "event_type": "trade",
                "event_time": 1720000025000,
                "symbol": "BTCUSDT",
                "trade_id": 1,
                "price": "65000.0",
                "quantity": "1.0",
                "trade_time": 1720000025000,
                "is_buyer_maker": False,
            },
            # Trade 2: Duplicate of trade 1 (should be ignored by deduplication)
            {
                "event_type": "trade",
                "event_time": 1720000025000,
                "symbol": "BTCUSDT",
                "trade_id": 1,
                "price": "65000.0",
                "quantity": "1.0",
                "trade_time": 1720000025000,
                "is_buyer_maker": False,
            },
            # Trade 3: Maker buy / Selling pressure (negative OFI)
            {
                "event_type": "trade",
                "event_time": 1720000035000,
                "symbol": "BTCUSDT",
                "trade_id": 2,
                "price": "65010.0",
                "quantity": "2.0",
                "trade_time": 1720000035000,
                "is_buyer_maker": True,
            },
            # Trade 4: Taker buy (positive OFI)
            {
                "event_type": "trade",
                "event_time": 1720000045000,
                "symbol": "BTCUSDT",
                "trade_id": 3,
                "price": "65020.0",
                "quantity": "3.0",
                "trade_time": 1720000045000,
                "is_buyer_maker": False,
            },
            # Trade 5: Late event but within watermark (should be included)
            {
                "event_type": "trade",
                "event_time": 1720000040000,
                "symbol": "BTCUSDT",
                "trade_id": 4,
                "price": "65005.0",
                "quantity": "4.0",
                "trade_time": 1720000040000,
                "is_buyer_maker": True,
            },
            # Trade 6: Advanced event that pushes watermark to finalise the first window
            {
                "event_type": "trade",
                "event_time": 1720000095000,
                "symbol": "BTCUSDT",
                "trade_id": 5,
                "price": "65000.0",
                "quantity": "1.0",
                "trade_time": 1720000095000,
                "is_buyer_maker": False,
            },
        ]

        # Produce events to Kafka
        producer = Producer(
            {"bootstrap.servers": stream_config.kafka_bootstrap_servers}
        )
        for t in trades:
            producer.produce(
                topic=stream_config.kafka_topic_trades,
                key="BTCUSDT",
                value=json.dumps(t).encode("utf-8"),
            )
            # Ensure chronological sorting on Kafka partitions
            time.sleep(0.05)
        producer.flush()

        # Set up a consumer to listen on the output Kafka topic
        consumer = Consumer(
            {
                "bootstrap.servers": stream_config.kafka_bootstrap_servers,
                "group.id": "test-vwap-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([stream_config.kafka_topic_agg_vwap])

        # 2. Run Spark Structured Streaming job
        queries = run_vwap_stream(stream_config)

        # Allow stream to process the micro-batch and finalize the window
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
            # Stop streaming queries
            for q in queries:
                q.stop()
            # Clean up singleton SparkSession so next tests get fresh settings
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is not None:
                spark.stop()

        # 3. Assertions
        assert kafka_received is not None, (
            "Did not receive aggregated VWAP event on Kafka topic"
        )
        assert kafka_received["symbol"] == "BTCUSDT"
        assert (
            kafka_received["trade_count"] == 4
        )  # 4 unique events, duplicate is dropped

        # VWAP math check: (65000*1 + 65010*2 + 65020*3 + 65005*4) / (1+2+3+4) = 65010.0
        assert pytest.approx(kafka_received["vwap"]) == 65010.0

        # OFI math check: Buys (1 + 3) - Sells (2 + 4) = 4 - 6 = -2.0
        assert pytest.approx(kafka_received["order_flow_imbalance"]) == -2.0

        # Volatility check: stddev([65000, 65010, 65020, 65005]) ≈ 8.539125
        assert pytest.approx(kafka_received["price_volatility"], abs=1e-3) == 8.5391

        # Verify Delta Lake sink output by listing files in MinIO
        s3 = make_s3_client(
            endpoint=stream_config.minio_endpoint,
            access_key=stream_config.minio_access_key,
            secret_key=stream_config.minio_secret_key,
        )
        objects = s3.list_objects_v2(
            Bucket=stream_config.minio_bucket_silver,
            Prefix="vwap_stream/",
        )
        keys = [obj["Key"] for obj in objects.get("Contents", [])]

        # Check if Delta transaction logs and parquet files were created
        assert any("_delta_log" in k for k in keys), "Delta transaction log missing"
        assert any(k.endswith(".parquet") for k in keys), "Parquet data file missing"
