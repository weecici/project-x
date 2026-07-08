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
- Updated pyproject.toml — full tool config (ruff 88-char, double quotes; mypy strict; pytest asyncio auto)
- Updated .gitignore — added .env, Parquet, pytest/mypy caches, MLflow dirs

## [2026-07-07] layout-refactor | src layout & clean structure

**Structure Assessment & Packaging**
- Moved `ingestion/` to `src/ingestion/` to adopt standard Python packaging `src/` layout (see ADR-005).
- Relocated future python packages (`batch`, `streaming`, `ml`, `orchestration`) under `src/` to isolate code from tool config files in the project root.
- Grouped Swarm, Kubernetes, Prometheus/Grafana dashboards, and metadata configurations under a single nested `infra/` folder.
- Configured namespace packaging in `pyproject.toml` with `uv_build` backend.

**Wiki**
- Created `adr-005-src-layout.md` explaining the layout refactoring.
- Updated `project-structure.md` in the wiki to document the standard `src/` layout.
- Corrected decisions folder paths in `phase.md` and `INDEX.md`.

## [2026-07-07] phase-2 | Batch + Lake Maturation

**Structural fixes**
- Moved `src/ingestion/utils/logging.py` and `retry.py` to `src/utils/` (new cross-phase utility package); deleted `ingestion/utils/`.
- Created `src/utils/storage.py` — shared `make_s3_client()` factory used by both `ingestion.writer` and `batch.backfill`.
- Updated all import paths in `ws_client.py`, `run_producer.py`, `run_lake_writer.py`, `lake_writer.py`.
- Fixed `--cov=src` in `pyproject.toml` (was `--cov=ingestion`).
- Fixed stale AGENTS.md state line; added Phase 2 commands and architecture block.
- Added Phase 2 env vars to `.env.example`.

**Batch package — `src/batch/`**
- Created `batch/config.py` — `BatchConfig` with Binance REST, MinIO, and PySpark settings; `backfill_start_date` validated as ISO-8601.
- Created `batch/models.py` — `KlineRow` with `from_api_list()` classmethod; `Decimal` precision; frozen model.
- Created `batch/backfill/binance_rest.py` — async paginated REST fetcher; weight-header rate-limit guard; chunk-by-chunk Parquet write to bronze; uses `utils.retry.async_retry`.
- Created `batch/silver/kline_transformer.py` — PySpark `local[*]`; explicit schema read; Decimal casting; window dedup on `(symbol, interval, open_time)`; rejected rows to `silver/klines_rejected/`; partitioned Snappy Parquet write.
- Created `batch/run_backfill.py` + `batch/run_silver.py` — CLI entrypoints (`uv run backfill`, `uv run silver`).

**Dependencies added**
- Runtime: `httpx>=0.28.0`, `pyspark>=3.5.0` (installed: 4.1.2)
- Dev: `pytest-httpx>=0.35.0`

**Tests**
- Created `tests/unit/utils/test_retry.py` — 3 tests for `async_retry` (success, transient recovery, exhaustion).
- Created `tests/unit/batch/test_batch_config.py` — 7 tests for `BatchConfig`.
- Created `tests/unit/batch/test_batch_models.py` — 5 tests for `KlineRow.from_api_list`.
- Created `tests/unit/batch/test_binance_rest.py` — 4 tests for `fetch_klines` using `pytest-httpx` mocks.
- Created `tests/integration/test_silver_spark.py` — PySpark silver job integration test (testcontainers MinIO).
- Created `tests/e2e/test_phase2_backfill.py` — full pipeline E2E (REST → bronze → silver).

**Wiki**
- Created ADR-006 (`src/utils/` package rationale).
- Created ADR-007 (PySpark `local[*]` mode + Delta Lake deferral).
- Created ADR-008 (`httpx` over `requests`/`aiohttp`).
- Updated `project-structure.md` to reflect Phase 2 full layout.
- Updated `INDEX.md` with ADRs 006–008.
