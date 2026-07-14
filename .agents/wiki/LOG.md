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

## [2026-07-09] phase-3 | OLAP + dbt

**Structural fixes**
- Corrected all stale `.wiki/` → `.agents/wiki/` path references in AGENTS.md and project-structure.md.
- Corrected `dbt_project/` → `dbt/` reference in AGENTS.md planned block.
- Updated AGENTS.md state line to "Phase 2 complete. Phase 3 (OLAP + dbt) in progress."
- Updated AGENTS.md architecture block to show Phases 1–3 built modules.
- Updated `justfile` with all phase shortcuts (backfill, silver, load-olap, dbt-run, dbt-test).

**Infrastructure**
- Added `clickhouse` service to `docker-compose.yaml` (clickhouse/clickhouse-server:25-alpine, mem_limit 512m, HTTP :8123).
- Added `clickhouse_data` named volume.
- Created `infra/clickhouse/init/01_schema.sql` — DDL for `crypto.klines_raw` (ReplacingMergeTree).

**`src/olap/` — MinIO silver → ClickHouse loader**
- Created `olap/config.py` — `OlapConfig` (ClickHouse + MinIO silver settings).
- Created `olap/loader.py` — paginated S3 listing + `clickhouse_connect.insert_arrow()` zero-copy insert; idempotent via ReplacingMergeTree.
- Created `olap/run_loader.py` — CLI entrypoint (`uv run load-olap`).

**Dependencies**
- Runtime: `clickhouse-connect>=0.8.0`
- Dev: `testcontainers[clickhouse]` (added to testcontainers extras)

**dbt project (`dbt/`)**
- Created `dbt_project.yml` — staging (view) + gold (table) materialisations.
- Created `profiles.yml` — ClickHouse HTTP driver, all env-var based.
- Created `packages.yml` — `dbt_utils` for expression tests.
- Created `macros/generate_schema_name.sql` — forces all models into single `crypto` DB.
- Created `models/sources.yml` — `crypto.klines_raw` source with column tests.
- Created `models/staging/_staging.yml` + `stg_klines.sql` — typed view, stable contract.
- Created `models/gold/_gold.yml` — docs + `high >= low`, `volume >= 0` tests.
- Created `models/gold/ohlcv_daily.sql` — daily OHLCV (argMin/argMax + toDate).
- Created `models/gold/ohlcv_hourly.sql` — hourly OHLCV (toStartOfHour).
- Created `models/gold/price_returns.sql` — log returns via ClickHouse `neighbor()`.

**Tests**
- Created `tests/unit/olap/test_olap_config.py` — 5 tests for OlapConfig.
- Created `tests/integration/test_olap_loader.py` — 3 integration tests (testcontainers ClickHouse + MinIO): row count, queryability, idempotency.
- All 57 unit tests green.

**Wiki**
- Created ADR-009 (ClickHouse over DuckDB — rationale, table design, dbt schema strategy).
- Updated `INDEX.md` with ADR-009.
- Fully rewrote `project-structure.md` to reflect Phase 3 layout with correct paths.

## [2026-07-12] phase-3-refinement | dbt reorganise, lag window, OOM resolution

**dbt Refactoring & Styling**
- Moved facts from `dbt/models/gold/` to `dbt/models/marts/` and renamed models to follow naming standards:
  - `fct_daily_klines.sql` (renamed from `fct_ohlcv_daily.sql`)
  - `fct_hourly_klines.sql` (renamed from `fct_ohlcv_hourly.sql`)
  - `fct_kline_returns.sql` (renamed from `fct_price_returns.sql`)
- Deleted the empty `dbt/models/gold/` directory.
- Re-organized staging models under `dbt/models/staging/crypto/`:
  - `sources.yml` (renamed from `_sources.yml`)
  - `staging.yml` (renamed from `_stg_crypto__models.yml`)
- Created `dbt/models/marts/marts.yml` for fact model tests and documentation.
- Replaced ClickHouse's deprecated `neighbor()` with standard SQL `lag` + `nullIf` window functions in `fct_kline_returns.sql`.
- Added the `FINAL` modifier to staging view source references to guarantee correct deduplication of keys during queries.

**OLAP Loader & Container Infrastructure**
- Replaced folder-based DDL file with python-driven `KLINES_RAW_DDL` block in `src/olap/schema.py` and dynamic database/table initialization in `src/olap/loader.py`.
- Formatted PyArrow tables in `loader.py` to use `_SILVER_SCHEMA` casting, aligning PyArrow timestamp and decimal types with ClickHouse fields and preventing test data merge errors.
- Resolved local OOM errors by limiting dbt concurrency (`threads: 1`), capping ClickHouse query thread count (`<max_threads>2</max_threads>` in `custom-users.xml`), and raising the container memory limit to `2048m` in `docker-compose.yaml`.

**Wiki**
- Updated `project-structure.md` in the wiki to document the refactored directory structure.

## [2026-07-14] phase-4-implementation | PySpark Structured Streaming

**Stream Processing Implementation**
- Created `src/streaming/config.py` with validated `StreamingConfig` using Pydantic settings.
- Developed stateless `ohlcv_stream.py` aggregating raw Binance closed klines to Delta Lake (`s3a://silver/klines_stream`) and Kafka topic (`agg.klines`).
- Developed stateful `vwap_stream.py` with 10s watermarking and dropDuplicatesWithinWatermark stateful deduplication. Computes 1m rolling VWAP, OFI, price volatility, and trade count, sinking to Delta Lake (`s3a://silver/vwap_stream`) and Kafka topic (`agg.vwap`).
- Implemented CLI scripts `stream-ohlcv` and `stream-vwap` with logging initialization and graceful SparkSession query termination.
- Enabled regex topic subscriptions (`subscribePattern`) in Spark streaming readers to allow graceful startups prior to Kafka topics being created.
- Integrated Delta Lake packages `io.delta:delta-spark_2.13:4.3.1` (Python 3.13 / Spark 4.0.0 compatible) in Spark sessions.

**Tests & Validation**
- Created unit tests `test_streaming_config.py`.
- Developed integration tests `test_stream_ohlcv.py` and `test_stream_vwap.py` using Kafka + MinIO testcontainers, asserting aggregate accuracy and window boundaries.
- Handled active singleton JVM SparkSession teardown during test suites to prevent port conflicts.

**Wiki & Documentation**
- Created ADR-010 detailing the architectural pivot from Flink to PySpark Structured Streaming due to Python 3.13 limitations.
- Updated `INDEX.md` and `project-structure.md` to document the completed Phase 4 streaming layout.
