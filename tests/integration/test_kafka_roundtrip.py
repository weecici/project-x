"""Integration tests for the Kafka produce/consume roundtrip.

Uses ``testcontainers`` to spin up a real Kafka broker in Docker.
No mocking — validates that the confluent-kafka client correctly
produces and consumes JSON messages end-to-end.

Requires: Docker daemon running.
"""

from __future__ import annotations

import json

import pytest
from confluent_kafka import Consumer, Producer
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="module")
def kafka_container() -> KafkaContainer:
    """Start a Kafka testcontainer for the duration of the module."""
    with KafkaContainer() as kafka:
        yield kafka


@pytest.mark.integration
class TestKafkaRoundtrip:
    """End-to-end Kafka produce/consume tests against a real broker."""

    def test_produced_message_is_received_by_consumer(
        self, kafka_container: KafkaContainer
    ) -> None:
        """A produced JSON message is fully received and deserialised."""
        # Arrange
        bootstrap = kafka_container.get_bootstrap_server()
        topic = "test.roundtrip"
        payload = {"symbol": "BTCUSDT", "price": "65000.00", "quantity": "0.001"}

        producer = Producer({"bootstrap.servers": bootstrap})
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "test-roundtrip-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])

        # Act
        producer.produce(topic=topic, value=json.dumps(payload).encode("utf-8"))
        producer.flush(timeout=10)

        received: dict[str, str] | None = None
        for _ in range(20):
            msg = consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                val = msg.value()
                if val is not None:
                    received = json.loads(val.decode("utf-8"))
                    break

        consumer.close()

        # Assert
        assert received is not None, "No message received within timeout"
        assert received["symbol"] == "BTCUSDT"
        assert received["price"] == "65000.00"
        assert received["quantity"] == "0.001"

    def test_multiple_messages_maintain_order_within_partition(
        self, kafka_container: KafkaContainer
    ) -> None:
        """Messages with the same key arrive in the order they were produced."""
        # Arrange
        bootstrap = kafka_container.get_bootstrap_server()
        topic = "test.ordering"
        key = b"BTCUSDT"
        messages = [{"seq": i, "price": f"{65000 + i}.00"} for i in range(5)]

        producer = Producer({"bootstrap.servers": bootstrap})
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "test-ordering-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])

        # Act
        for message_payload in messages:
            producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(message_payload).encode("utf-8"),
            )
        producer.flush(timeout=10)

        received: list[dict[str, object]] = []
        for _ in range(30):
            msg = consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                val = msg.value()
                if val is not None:
                    received.append(json.loads(val.decode("utf-8")))
            if len(received) == len(messages):
                break

        consumer.close()

        # Assert
        assert len(received) == len(messages), "Did not receive all messages"
        seq_values = [int(str(r["seq"])) for r in received]
        assert seq_values == list(range(5)), "Messages received out of order"
