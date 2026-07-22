# Crypto Platform

End-to-end crypto market intelligence platform — live + historical data ingestion, lakehouse storage, OLAP analytics, and ML price-movement prediction.

```mermaid
flowchart TB
    subgraph SRC["Data Sources"]
        WS["Binance WebSocket"]
        REST["Binance REST API"]
    end

    subgraph INGEST["Ingestion"]
        PROD["Python Producer"]
        KAFKA["Apache Kafka\nKRaft"]
    end

    subgraph BATCH["Batch"]
        BF["REST Backfill"]
        SPARK["PySpark"]
    end

    subgraph STREAM["Stream Processing"]
        SPARK_STREAM["PySpark Structured\nStreaming"]
    end

    subgraph LAKE["Lake (Medallion)"]
        BRONZE["Bronze\n(Raw Parquet)"]
        SILVER["Silver\n(Parquet + Delta)"]
    end

    subgraph OLAP["OLAP + Analytics"]
        LOADER["OLAP Loader"]
        CH["ClickHouse"]
        DBT["dbt Models"]
    end

    WS --> PROD --> KAFKA --> BRONZE
    KAFKA --> SPARK_STREAM
    REST --> BF --> BRONZE
    BRONZE --> SPARK --> SILVER
    SPARK_STREAM --> SILVER

    SILVER --> LOADER --> CH
    CH --> DBT

    style OLAP fill:#e3f2fd,stroke:#1976d2
```

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/yourusername/crypto-platform.git
cd crypto-platform
uv sync

# 2. Start infrastructure
docker compose up -d

# 3. Run the live pipeline
uv run produce        # Terminal 1: WS → Kafka
uv run write-lake     # Terminal 2: Kafka → MinIO

# 4. Run batch pipeline
uv run backfill       # Historical data → bronze
uv run silver         # Bronze → silver transformation

# 5. Load into ClickHouse
uv run load-olap      # Silver → ClickHouse

# 6. Build gold tables
uv run dbt deps       # Install dbt packages
uv run dbt run        # Run all dbt models
uv run dbt test       # Run dbt tests

# 7. Start streaming (Terminal 3 + 4)
uv run stream-ohlcv   # Kafka → OHLCV Delta + Kafka
uv run stream-vwap    # Kafka → VWAP Delta + Kafka
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Runtime | Python 3.13, uv |
| Streaming | Apache Kafka (KRaft), confluent-kafka, PySpark Structured Streaming |
| Batch | PySpark, httpx |
| Storage | MinIO (S3-compatible), Parquet, Delta Lake |
| OLAP | ClickHouse, clickhouse-connect |
| Transforms | dbt, dbt-clickhouse |
| Validation | Pydantic v2 |
| Infrastructure | Docker Compose |
| Testing | pytest, testcontainers |
| Linting | ruff, mypy --strict |

## Project Structure

```
src/
├── utils/           # Shared utilities (logging, retry, storage)
├── ingestion/       # Live WS → Kafka → MinIO pipeline
│   ├── producer/    # WebSocket → Kafka producer
│   └── writer/      # Kafka → MinIO lake writer
├── batch/           # REST backfill + PySpark silver transformer
│   ├── backfill/    # Binance REST client
│   └── silver/      # PySpark transformer
├── olap/            # MinIO silver → ClickHouse loader
└── streaming/       # PySpark Structured Streaming
    └── jobs/        # OHLCV + VWAP streaming jobs

dbt/
├── models/
│   ├── staging/     # Silver → staging views
│   └── marts/       # Gold aggregated tables
└── macros/          # Schema naming overrides

tests/
├── unit/            # Fast, no Docker
├── integration/     # Docker required (testcontainers)
└── e2e/             # Full stack
```

## Documentation

Full documentation is available at [docs/](docs/):

- [Architecture](docs/architecture/overview.md) — System design and data flow
- [Configuration](docs/guides/configuration.md) — All environment variables
- [CLI Reference](docs/reference/cli.md) — All commands
- [API Reference](docs/reference/api-utils.md) — Auto-generated from docstrings
- [dbt Models](docs/guides/dbt.md) — dbt model catalog and usage
- [Roadmap](docs/development/roadmap.md) — 10-phase build plan

## Development

```bash
# Run tests
uv run pytest tests/unit/ -v

# Lint and format
uv run ruff check src/ --fix
uv run ruff format src/

# Type check
uv run mypy src/

# Build docs locally
uv run mkdocs serve

# Just shortcuts
just pc              # Pre-commit hooks
just check           # Lint
just format          # Format
just mypy            # Type check
just docs            # Serve docs
```

## Current Status

| Phase | Name | Status |
|:-----:|------|:------:|
| 1 | Foundation + Ingestion | Done |
| 2 | Batch + Lake Maturation | Done |
| 3 | OLAP + dbt | Done |
| 4 | Stream Processing | Done |
| 5 | Semantic Layer + BI | Done |
| 6 | Orchestration + Governance | Planned |
| 7 | Observability | Planned |
| 8 | ML Pipeline | Planned |
| 9 | ML Serving | Planned |
| 10 | CI/CD + Deploy | Planned |

## License

MIT
