"""E2E smoke tests for the Phase 1 pipeline.

Validates the full data flow:
    Binance WS → Kafka (raw.trades, raw.klines) → MinIO bronze/

Prerequisites (run before this suite):
    docker compose up -d
    uv run python ingestion/run_producer.py &
    uv run python ingestion/run_lake_writer.py &

Run with:
    uv run pytest tests/e2e/ -v -m e2e
"""

from __future__ import annotations

import json
import time
from collections.abc import Generator

import boto3
import pytest
from botocore.config import Config
from confluent_kafka import Consumer
from mypy_boto3_s3 import S3Client

# ---------------------------------------------------------------------------
# Constants — match defaults in .env.example
# ---------------------------------------------------------------------------
_KAFKA_BOOTSTRAP = "localhost:9094"
_MINIO_ENDPOINT = "http://localhost:9000"
_MINIO_ACCESS_KEY = "minioadmin"
_MINIO_SECRET_KEY = "minioadmin"
_BRONZE_BUCKET = "bronze"
_TRADE_TOPIC = "raw.trades"
_KLINE_TOPIC = "raw.klines"


# ---------------------------------------------------------------------------
# Module-level fixtures (connect once per session)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def kafka_consumer() -> Generator[Consumer]:
    """Return a Kafka consumer subscribed to both raw topics."""
    consumer = Consumer(
        {
            "bootstrap.servers": _KAFKA_BOOTSTRAP,
            "group.id": "e2e-smoke-test",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([_TRADE_TOPIC, _KLINE_TOPIC])
    yield consumer
    consumer.close()


@pytest.fixture(scope="module")
def s3_client() -> S3Client:
    """Return a boto3 S3 client pointed at local MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=_MINIO_ENDPOINT,
        aws_access_key_id=_MINIO_ACCESS_KEY,
        aws_secret_access_key=_MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_kafka_receives_trade_events(kafka_consumer: Consumer) -> None:
    """raw.trades receives live TradeEvent messages from the producer.

    Waits up to 30 seconds for at least 5 messages. Each message must
    contain the normalised Python-name fields produced by the ws_client.
    """
    # Arrange
    received: list[dict[str, object]] = []
    deadline = time.monotonic() + 30

    # Act
    while time.monotonic() < deadline and len(received) < 5:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg is not None and not msg.error() and msg.topic() == _TRADE_TOPIC:
            val = msg.value()
            if val is not None:
                received.append(json.loads(val.decode("utf-8")))

    # Assert
    assert len(received) >= 5, (
        f"Expected ≥5 trade messages within 30 s, received {len(received)}"
    )
    for event in received:
        assert "symbol" in event, "Normalised 'symbol' field missing"
        assert "price" in event, "Normalised 'price' field missing"
        assert "quantity" in event, "Normalised 'quantity' field missing"
        assert "trade_id" in event, "Normalised 'trade_id' field missing"


@pytest.mark.e2e
def test_kafka_receives_kline_events(kafka_consumer: Consumer) -> None:
    """raw.klines receives live KlineEvent messages from the producer."""
    # Arrange
    received: list[dict[str, object]] = []
    deadline = time.monotonic() + 90  # klines arrive every ~60 s

    # Act
    while time.monotonic() < deadline and len(received) < 2:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg is not None and not msg.error() and msg.topic() == _KLINE_TOPIC:
            val = msg.value()
            if val is not None:
                received.append(json.loads(val.decode("utf-8")))

    # Assert
    assert len(received) >= 2, (
        f"Expected ≥2 kline messages within 90 s, received {len(received)}"
    )
    for event in received:
        assert "symbol" in event
        assert "kline" in event
        kline = event["kline"]
        assert isinstance(kline, dict)
        assert "open" in kline


@pytest.mark.e2e
def test_minio_bronze_bucket_receives_parquet_files(s3_client: S3Client) -> None:
    """bronze/ bucket receives Parquet files within one flush interval (≤65 s).

    The lake writer flushes every 30 s or 1 000 rows; we wait 65 s to
    account for startup latency.
    """
    # Arrange
    deadline = time.monotonic() + 65
    found: list[dict[str, object]] = []

    # Act
    while time.monotonic() < deadline and not found:
        response = s3_client.list_objects_v2(
            Bucket=_BRONZE_BUCKET,
            Prefix="trades/",
        )
        found = response.get("Contents", [])  # type: ignore[assignment]
        if not found:
            time.sleep(5)

    # Assert
    assert found, (
        "No Parquet files appeared in bronze/trades/ within 65 seconds. "
        "Ensure both run_producer.py and run_lake_writer.py are running."
    )
    assert all(str(obj.get("Key", "")).endswith(".parquet") for obj in found), (
        "Non-Parquet file found in bronze/trades/"
    )
