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
| **PySpark** | 3.5+ | Distributed batch + stream processing | Dedup, type casting, partitioning, Structured Streaming |
| **delta-spark** | 4.0+ | ACID lake storage on Spark | Open table format, exactly-once writes, Spark integration |
| **pyarrow** | 24+ | Parquet I/O + S3 filesystem | Arrow-native Parquet, MinIO/S3 integration |
| **clickhouse-connect** | 0.8+ | ClickHouse HTTP client | Zero-copy Arrow inserts, lightweight |
| **dbt-clickhouse** | 1.10+ | dbt adapter for ClickHouse | SQL-based transformations on ClickHouse |
| **minio** | 7.x | S3-compatible object storage client | Python SDK for MinIO |
| **gspread** | 6.x | Google Sheets API client | BI exporter syncs CSV data to Google Sheets |
| **pandas** | 3.x | DataFrame operations | BI exporter CSV processing |
| **openlineage-python** | 1.51+ | OpenLineage event emission | Standard lineage event format for governance |
| **loguru** | 0.7+ | Structured logging | Simplified logging with structured output |
| **rich** | 15+ | Terminal formatting | Beautiful terminal output for CLI |
| **tenacity** | 9.x | Retry library | Exponential backoff, configurable retries |
| **boto3** | 1.43+ | AWS SDK | S3-compatible operations |
| **python-dotenv** | 1.x | `.env` file loading | Standard `.env` file support |
| **PyTorch** | 2.x | Deep learning framework | LSTM model training, CUDA RTX 3050 Ti support |
| **MLflow** | 2.x | Experiment tracking | Experiment logging, model registry, artifact storage |
| **Numba** | 0.61+ | JIT compilation | EMA, RSI, MACD acceleration (up to 97x speedup) |
| **pandas-ta** | 0.3+ | Technical indicators | RSI, MACD, Bollinger Bands as Pandas UDFs |
| **onnx** | 1.17+ | Model serialization | Cross-platform model export |
| **scikit-learn** | 1.6+ | ML utilities | Standard scaling, train/test splitting |
| **psutil** | 6.x | System monitoring | Training resource metrics logging |

## Infrastructure

| Service | Image | Purpose | Why |
|---------|-------|---------|-----|
| **Apache Kafka** | `apache/kafka` | Message broker | Durable, partitioned, high-throughput event streaming |
| **Kafka (KRaft mode)** | — | Metadata management | No ZooKeeper dependency, simpler single-node setup |
| **MinIO** | `minio/minio` | S3-compatible object store | Local data lake storage, Hive-partitioned Parquet |
| **ClickHouse** | `clickhouse/clickhouse-server:head-alpine` | OLAP database | Columnar analytics, ReplacingMergeTree dedup, fast inserts |
| **mc-init** | `minio/mc` | Bucket initialization | Creates bronze/silver/gold buckets on startup |
| **Cube.js** | `cubejs/cube` | Semantic layer | Metrics API on ClickHouse gold tables, pre-aggregations |
| **PostgreSQL** | `postgres:alpine` | Airflow + MLflow metadata DB | Lightweight, 256MB cap |
| **Apache Airflow** | *(native Python)* | Workflow orchestration | DAG-based scheduling, LocalExecutor, runs via `just airflow-up` |
| **MLflow** | `ghcr.io/mlflow/mlflow:v2.21.3` | ML experiment tracking | Tracking server + PostgreSQL backend + MinIO artifacts (`--profile ml`) |
| **Prometheus** | `prom/prometheus:latest` | Metrics collection + TSDB | 15s scrape interval, 7 scrape jobs, alert rules |
| **Grafana** | `grafana/grafana:latest` | Dashboards + alerting | Dashboard-as-code (`allowUiUpdates: false`), provisioned datasources |
| **Loki** | `grafana/loki:latest` | Log aggregation | Filesystem TSDB, schema v13, 7-day retention |
| **Grafana Alloy** | `grafana/alloy:latest` | Log collector | Replaces EOL Promtail, reads Docker socket |
| **AlertManager** | `prom/alertmanager:latest` | Alert routing | Symptom-based rules, 30s group wait, 5m group interval |
| **kafka-exporter** | `danielqsj/kafka-exporter:latest` | Kafka metrics | Consumer lag + partition offset via Kafka Admin API |
| **cAdvisor** | `gcr.io/cadvisor/cadvisor:latest` | Container metrics | Per-container CPU/RAM/Net/IO |
| **node-exporter** | `prom/node-exporter:latest` | Host metrics | CPU, RAM, disk utilization |
| **statsd-exporter** | `prom/statsd-exporter:latest` | StatsD bridge | Airflow StatsD → Prometheus metrics |

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
| **Parquet** | Lake storage (bronze, batch silver) | Columnar, compressed, schema-embedded, Hive-compatible |
| **Delta Lake** | Lake storage (streaming silver) | ACID transactions, exactly-once semantics, schema enforcement |
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
