# Wiki Log

Chronological record of wiki changes. Each entry uses the format: `## [YYYY-MM-DD] action | subject`

## [2026-07-05] init | Project setup

- Created `GUIDE.md` — full project blueprint (311 lines): crypto market intelligence platform with Kafka/Flink streaming, Spark/dbt batch pipeline, ClickHouse/Doris OLAP, Cube semantic layer, PyTorch model training with Triton/BentoML/FastAPI serving comparison
- Created `wiki/architecture/overview.md` — Mermaid system architecture diagram
- Created `wiki/architecture/breakdown.md` — component-by-component tool rationale (17 sections)
- Created `wiki/structure/phase.md` — 9-phase build order table
- Created `wiki/structure/project-structure.md` — planned directory layout
- Created `wiki/INDEX.md` — content catalog of all wiki pages
- Created `wiki/LOG.md` — this file
- Configured `pyproject.toml` — Python 3.13, uv package manager, no deps yet
- Created `main.py` — hello-world stub

## [2026-07-05] phase-1 | Foundation + Ingestion

**Stack assessment & modifications**
- Revised phase build order from 9 → 10 phases (split ML training/serving; added explicit CI/CD phase)
- Kafka: KRaft mode (no ZooKeeper) — see ADR-001
- Kafka Python client: `confluent-kafka` (dropped archived `kafka-python-ng`) — see ADR-002
- Flink deferred to Phase 4 — see ADR-003
- GPU profile documented: RTX 3050 Ti, 4 GB VRAM, CUDA 13.3, sm_86 — see ADR-004

**Infrastructure**
- Created `docker-compose.yml` — Kafka 3.9 KRaft + MinIO + kafka-ui + mc-init (creates bronze/silver/gold buckets)
- Created `.env.example` — all environment variables documented
- Created `.pre-commit-config.yaml` — ruff + mypy + pre-commit-hooks

**Python package — `ingestion/`**
- Created `ingestion/config.py` — pydantic-settings BaseSettings
- Created `ingestion/models.py` — `TradeEvent`, `KlineData`, `KlineEvent` (pydantic v2, Binance API aliases)
- Created `ingestion/utils/logging.py` — loguru structured JSON logging
- Created `ingestion/utils/retry.py` — `async_retry` decorator (tenacity exponential-jitter backoff)
- Created `ingestion/producer/ws_client.py` — async Binance WS → confluent-kafka producer (idempotent, DLQ routing)
- Created `ingestion/producer/lake_writer.py` — Kafka consumer → Snappy Parquet → MinIO bronze (hive-partitioned, manual commit)
- Created `ingestion/run_producer.py` + `ingestion/run_lake_writer.py` — CLI entrypoints

**Tests — `tests/`**
- Created `tests/unit/ingestion/test_models.py` — TradeEvent + KlineEvent unit tests (Arrange-Act-Assert, parametrize)
- Created `tests/unit/ingestion/test_config.py` — IngestionConfig defaults + env override tests
- Created `tests/integration/test_kafka_roundtrip.py` — testcontainers Kafka produce/consume integration tests
- Created `tests/integration/test_minio_writer.py` — testcontainers MinIO Parquet write/read integration tests
- Created `tests/e2e/test_phase1_pipeline.py` — full pipeline E2E smoke tests (against compose stack)
- Created `tests/conftest.py` — shared fixtures

**Wiki**
- Created `wiki/decisions/` directory with 4 ADRs (adr-001 through adr-004)
- Updated `wiki/structure/phase.md` — 10-phase table
- Updated `wiki/structure/project-structure.md` — full annotated directory layout
- Updated `wiki/INDEX.md` — added decisions section

**Toolchain**
- Installed runtime deps: `confluent-kafka`, `websockets`, `pydantic`, `pydantic-settings`, `pyarrow`, `boto3`, `loguru`, `tenacity`, `rich`
- Installed dev deps: `ruff`, `mypy`, `pytest`, `pytest-asyncio`, `pytest-cov`, `testcontainers[kafka,minio]`, `pre-commit`, `boto3-stubs[s3]`
- Updated `pyproject.toml` — full tool config (ruff 88-char, double quotes; mypy strict; pytest asyncio auto)
- Updated `.gitignore` — added `.env`, Parquet, pytest/mypy caches, MLflow dirs
