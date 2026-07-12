# Tech Stack

Every tool and library used in the platform, with version, purpose, and rationale.

## Runtime

| Tool | Version | Purpose | Why |
|------|---------|---------|-----|
| **Python** | 3.13 | Application runtime | Latest stable; improved type system, performance |
| **uv** | Latest | Package/dependency manager | 10–100x faster than pip; deterministic lockfile |
| **Docker** | 24+ | Container runtime | Isolates Kafka, MinIO, ClickHouse from host |
| **Docker Compose** | v2+ | Multi-container orchestration | Single `docker compose up` for full stack |

## Core Libraries

| Library | Version | Purpose | Why |
|---------|---------|---------|-----|
| **confluent-kafka** | 2.x | Kafka producer/consumer | Official Confluent client, librdkafka-backed (C performance) |
| **websockets** | 16.x | Binance WebSocket client | Async-first, lightweight, well-maintained |
| **httpx** | 0.28+ | Async HTTP client (REST API) | Async-native, connection pooling, rate limiting |
| **pydantic** | 2.x | Data validation + settings | Fast validation, type-safe models |
| **pydantic-settings** | 2.x | Environment variable config | `.env` + env var loading into Pydantic models |
| **PySpark** | 3.5+ | Distributed batch processing | Dedup, type casting, partitioning at scale |
| **pyarrow** | 24+ | Parquet I/O + S3 filesystem | Arrow-native Parquet, MinIO/S3 integration |
| **clickhouse-connect** | 0.8+ | ClickHouse HTTP client | Zero-copy Arrow inserts, lightweight |
| **dbt-clickhouse** | 1.10+ | dbt adapter for ClickHouse | SQL-based transformations on ClickHouse |
| **minio** | 7.x | S3-compatible object storage client | Python SDK for MinIO |
| **loguru** | 0.7+ | Structured logging | Simplified logging with structured output |
| **rich** | 15+ | Terminal formatting | Beautiful terminal output for CLI |
| **tenacity** | 9.x | Retry library | Exponential backoff, configurable retries |
| **boto3** | 1.43+ | AWS SDK | S3-compatible operations |
| **python-dotenv** | 1.x | `.env` file loading | Standard `.env` file support |

## Infrastructure

| Service | Image | Purpose | Why |
|---------|-------|---------|-----|
| **Apache Kafka** | `apache/kafka` | Message broker | Durable, partitioned, high-throughput event streaming |
| **Kafka (KRaft mode)** | — | Metadata management | No ZooKeeper dependency, simpler single-node setup |
| **MinIO** | `minio/minio` | S3-compatible object store | Local data lake storage, Hive-partitioned Parquet |
| **ClickHouse** | `clickhouse/clickhouse-server:head-alpine` | OLAP database | Columnar analytics, ReplacingMergeTree dedup, fast inserts |
| **mc-init** | `minio/mc` | Bucket initialization | Creates bronze/silver/gold buckets on startup |

## Development Tools

| Tool | Purpose | Config |
|------|---------|--------|
| **ruff** | Linting + formatting (replaces black, flake8, isort) | 88-char lines, double quotes |
| **mypy** | Static type checking | `--strict` mode, `ignore_missing_imports = true` |
| **pre-commit** | Git hook automation | Runs ruff + mypy on every commit |
| **pytest** | Test framework | `asyncio_mode=auto`, fixtures, parametrize |
| **pytest-asyncio** | Async test support | Auto mode for async test functions |
| **testcontainers** | Integration test infrastructure | Spins up Docker containers for Kafka/MinIO/ClickHouse |
| **dbt** | Data build tool | SQL-based transformations, testing, documentation |
| **just** | Command runner | Shortcut recipes for all CLI commands |

## Data Format

| Format | Usage | Why |
|--------|-------|-----|
| **Parquet** | Lake storage (bronze, silver) | Columnar, compressed, schema-embedded, Hive-compatible |
| **JSON** | WebSocket messages, Kafka payloads | Human-readable, native Binance format |
| **JSON Lines** | Structured logging | Newline-delimited JSON for log aggregation |
| **ClickHouse Native** | OLAP storage | Columnar, vectorized, ReplacingMergeTree engine |

## Python Version Pinning

The project pins Python 3.13 in `.python-version` and uses `uv` to manage it:

```bash
# uv auto-installs the pinned Python version
uv sync  # Creates .venv with Python 3.13
```

## Key Version Constraints

From `pyproject.toml`:

```
python = ">=3.13, <3.14"
```

This ensures compatibility with Python 3.13 features while preventing accidental upgrades to untested Python versions.
