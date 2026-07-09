# Tech Stack

Every tool and library used in the platform, with version, purpose, and rationale.

## Runtime

| Tool | Version | Purpose | Why |
|------|---------|---------|-----|
| **Python** | 3.13 | Application runtime | Latest stable; improved type system, performance |
| **uv** | Latest | Package/dependency manager | 10–100x faster than pip; deterministic lockfile |
| **Docker** | 24+ | Container runtime | Isolates Kafka, MinIO from host |
| **Docker Compose** | v2+ | Multi-container orchestration | Single `docker compose up` for full stack |

## Core Libraries

| Library | Version | Purpose | Why |
|---------|---------|---------|-----|
| **confluent-kafka** | 2.x | Kafka producer/consumer | Official Confluent client, librdkafka-backed (C performance) |
| **websockets** | 14.x | Binance WebSocket client | Async-first, lightweight, well-maintained |
| **httpx** | 0.28+ | Async HTTP client (REST API) | httpx is async-native, supports connection pooling and rate limiting |
| **pydantic** | 2.x | Data validation + settings | Fast validation, `.env` loading, type-safe models |
| **pydantic-settings** | 2.x | Environment variable config | Seamless `.env` + env var loading into Pydantic models |
| **PySpark** | 3.5+ | Distributed batch processing | Handles dedup, type casting, partitioning at scale |
| **pyarrow** | 18+ | Parquet I/O + S3 filesystem | Arrow-native Parquet, MinIO/S3 integration |
| **minio** | 7.x | S3-compatible object storage client | Python SDK for MinIO |
| **python-dotenv** | 1.x | `.env` file loading | Widespread standard, battle-tested |

## Infrastructure

| Service | Image | Purpose | Why |
|---------|-------|---------|-----|
| **Apache Kafka** | `bitnami/kafka` | Message broker | Durable, partitioned, high-throughput event streaming |
| **Kafka (KRaft mode)** | — | Metadata management | No ZooKeeper dependency, simpler single-node setup |
| **MinIO** | `minio/minio` | S3-compatible object store | Local data lake storage, Hive-partitioned Parquet |

## Development Tools

| Tool | Purpose | Config |
|------|---------|--------|
| **ruff** | Linting + formatting (replaces black, flake8, isort) | 88-char lines, double quotes |
| **mypy** | Static type checking | `--strict` mode, `ignore_missing_imports = true` |
| **pre-commit** | Git hook automation | Runs ruff + mypy on every commit |
| **pytest** | Test framework | `asyncio_mode=auto`, fixtures, parametrize |
| **pytest-asyncio** | Async test support | Auto mode for async test functions |
| **testcontainers** | Integration test infrastructure | Spins up Docker containers for Kafka/MinIO |

## Data Format

| Format | Usage | Why |
|--------|-------|-----|
| **Parquet** | Lake storage (bronze, silver) | Columnar, compressed, schema-embedded, Hive-compatible |
| **JSON** | WebSocket messages, Kafka payloads | Human-readable, native Binance format |
| **JSON Lines** | Structured logging | Newline-delimited JSON for log aggregation |

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
