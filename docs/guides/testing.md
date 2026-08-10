# Testing

The platform uses a three-tier testing strategy: unit, integration, and end-to-end.

## Test Structure

```
tests/
├── unit/
│   ├── ingestion/
│   │   ├── test_models.py              # Pydantic model validation
│   │   └── test_config.py              # IngestionConfig loading
│   ├── batch/
│   │   ├── test_batch_models.py        # Batch config/result models
│   │   ├── test_batch_config.py        # BatchConfig validation
│   │   └── test_binance_rest.py        # REST client unit tests
│   ├── streaming/
│   │   └── test_streaming_config.py    # StreamingConfig defaults + validation
│   ├── olap/
│   │   ├── test_loader_config.py       # OlapLoaderConfig defaults + overrides
│   │   └── test_exporter_config.py     # BiExporterConfig defaults + overrides
│   ├── orchestration/
│   │   ├── test_orchestration_config.py # OrchestrationConfig + GovernanceConfig
│   │   ├── test_dags_validation.py     # DAG structure validation (3 DAGs)
│   │   └── test_lineage.py             # Lineage manifest builder
│   ├── ml/
│   │   ├── test_ml_config.py           # FeatureConfig, TrainingConfig, OptimizationConfig
│   │   ├── test_numba_indicators.py    # Numba JIT EMA, RSI, MACD correctness
│   │   ├── test_model.py               # CryptoLSTM architecture + forward pass
│   │   └── test_dataset.py             # CryptoDataset + DataLoader
│   ├── observability/
│   │   ├── test_prometheus_config.py   # Prometheus scrape configs + rules
│   │   ├── test_loki_config.py         # Loki + AlertManager config validity
│   │   └── test_grafana_provisioning.py # Grafana datasources, dashboards, alerting
│   └── utils/
│       └── test_retry.py               # Retry decorator behavior
├── integration/
│   ├── test_stream_ohlcv.py            # OHLCV streaming (Kafka + MinIO testcontainers)
│   ├── test_stream_vwap.py             # VWAP streaming (Kafka + MinIO testcontainers)
│   ├── test_silver_spark.py            # PySpark silver transformation
│   ├── test_serving_loader.py          # ClickHouse loader (ClickHouse + MinIO testcontainers)
│   ├── test_serving_exporter.py        # Cube.js exporter integration
│   ├── test_orchestration_lineage.py   # End-to-end lineage manifest export
│   ├── test_minio_writer.py            # Kafka → MinIO lake writer
│   └── test_kafka_roundtrip.py         # Kafka producer/consumer roundtrip
└── e2e/
    ├── test_phase1_pipeline.py         # WS → Kafka → MinIO end-to-end
    └── test_phase2_backfill.py         # REST → bronze → silver end-to-end
```

## Running Tests

### Unit Tests (No Docker)

```bash
uv run pytest tests/unit/ -v
```

Fast, runs in seconds. Tests Pydantic models, config loading, and data validation.

### Integration Tests (Requires Docker)

```bash
uv run pytest tests/integration/ -v
```

Uses `testcontainers` to spin up real Kafka, MinIO, and ClickHouse instances. Takes ~30 seconds.

Streaming integration tests spin up Kafka + MinIO testcontainers, produce mock events, run the Spark Structured Streaming job, and verify output in both Delta Lake and Kafka. They handle SparkSession singleton teardown between tests to prevent port conflicts.

### End-to-End Tests (Requires Full Stack)

```bash
uv run pytest tests/e2e/ -v -m e2e
```

Tests the entire pipeline with running Kafka, MinIO, and real Binance API calls. Takes ~2–5 minutes.

### Run All Tests

```bash
uv run pytest -v
```

## Writing Tests

### Unit Test Example

```python
"""Tests for StreamingConfig defaults and validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from streaming.config import StreamingConfig


def test_default_kafka_topics() -> None:
    """Default Kafka topics match the design specifications."""
    config = StreamingConfig(_env_file=None)  # type: ignore[call-arg]

    assert config.kafka_topic_trades == "raw.trades"
    assert config.kafka_topic_klines == "raw.klines"
    assert config.kafka_topic_agg_klines == "agg.klines"
    assert config.kafka_topic_agg_vwap == "agg.vwap"


def test_negative_watermark_raises() -> None:
    """A negative watermark delay must raise a ValidationError."""
    with pytest.raises(ValidationError):
        StreamingConfig(
            _env_file=None,  # type: ignore[call-arg]
            stream_watermark_delay_seconds=-5,
        )
```

### Integration Test Example

```python
"""Tests for Kafka producer with testcontainers."""

import pytest
from testcontainers.kafka import KafkaContainer


@pytest.fixture
def kafka_broker():
    """Spin up a real Kafka broker."""
    with KafkaContainer() as kafka:
        yield kafka.get_bootstrap_server()
```

### Test Conventions

- **One assertion focus** per test
- **Arrange → Act → Assert** structure
- **Descriptive names**: `test_<what>_<condition>_<expected>`
- **`pytest.mark.parametrize`** for variations instead of copy-paste
- **`pytest.mark.asyncio`** for async tests (auto mode)
- Config tests use `_env_file=None` to isolate from `.env` file

## Test Configuration

### pytest.ini / pyproject.toml

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
markers = [
    "e2e: End-to-end tests (requires full stack)",
]
filterwarnings = [
    "ignore::DeprecationWarning:unittest.mock.*:",
]
```

### Fixtures

Shared fixtures are in `conftest.py` files:

```
tests/conftest.py              # Shared fixtures
tests/unit/conftest.py         # Unit test fixtures
tests/integration/conftest.py  # Docker container fixtures
tests/e2e/conftest.py          # Full stack fixtures
```

## Coverage

Generate coverage reports:

```bash
uv run pytest --cov=src --cov-report=term-missing
```

Coverage targets:

- Unit tests: >90% line coverage
- Integration tests: >70% for I/O components
- E2E tests: Critical path coverage

## CI Integration

Tests run automatically via GitHub Actions on every push and PR:

```yaml
# .github/workflows/test.yaml
- name: Unit Tests
  run: uv run pytest tests/unit/ -v

- name: Integration Tests
  run: uv run pytest tests/integration/ -v
```

See [Contributing Guide](../development/contributing.md) for PR requirements.
