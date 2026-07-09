# Testing

The platform uses a three-tier testing strategy: unit, integration, and end-to-end.

## Test Structure

```
tests/
├── unit/                           # Fast, no Docker required
│   ├── test_ingestion_models.py    # Pydantic model validation
│   ├── test_batch_models.py        # Batch config/result models
│   └── test_config.py              # Configuration loading
├── integration/                    # Requires Docker
│   ├── test_producer.py            # Kafka producer with testcontainers
│   ├── test_lake_writer.py         # Kafka → MinIO with testcontainers
│   └── test_backfill.py            # REST client with mock server
└── e2e/                            # Requires full compose stack
    ├── test_live_pipeline.py       # WS → Kafka → MinIO end-to-end
    └── test_batch_pipeline.py      # REST → bronze → silver end-to-end
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

Uses `testcontainers` to spin up real Kafka and MinIO instances. Takes ~30 seconds.

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
"""Tests for ingestion models."""

from src.ingestion.models import Trade, Kline


def test_trade_model_parses_correctly():
    """Trade model accepts valid Binance trade data."""
    data = {
        "type": "trade",
        "symbol": "BTCUSDT",
        "trade_id": 12345,
        "price": "50000.00",
        "quantity": "0.001",
        "trade_time": 1694000000000,
        "is_buyer_maker": False,
        "event_time": 1694000000001,
    }
    trade = Trade.model_validate(data)
    assert trade.symbol == "BTCUSDT"
    assert trade.price == "50000.00"


def test_trade_model_rejects_missing_fields():
    """Trade model raises ValidationError for incomplete data."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Trade.model_validate({"type": "trade", "symbol": "BTCUSDT"})
```

### Integration Test Example

```python
"""Tests for Kafka producer with testcontainers."""

import pytest
from testcontainers.kafka import KafkaContainer


@pytest.fixture
def kafka_broker():
    """Spin up a real Kafka broker."""
    with KafkaContainer("bitnami/kafka:latest") as kafka:
        yield kafka.get_bootstrap_server()
```

### Test Conventions

- **One assertion focus** per test
- **Arrange → Act → Assert** structure
- **Descriptive names**: `test_<what>_<condition>_<expected>`
- **`pytest.mark.parametrize`** for variations instead of copy-paste
- **`pytest.mark.asyncio`** for async tests (auto mode)

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
