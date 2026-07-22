# Roadmap

10-phase build plan from ingestion to production deployment.

## Phase Overview

| Phase | Name | Status | Description |
|:-----:|------|:------:|-------------|
| 1 | Foundation + Ingestion | :material-check-circle:{ style="color: green" } | Live WS → Kafka → MinIO lake |
| 2 | Batch + Lake Maturation | :material-check-circle:{ style="color: green" } | REST backfill, PySpark silver |
| 3 | OLAP + dbt | :material-check-circle:{ style="color: green" } | ClickHouse, dbt staging + gold marts |
| 4 | Stream Processing | :material-check-circle:{ style="color: green" } | PySpark Structured Streaming |
| 5 | Semantic Layer + BI | :material-clock-outline: | Metrics store, dashboard integration |
| 6 | Orchestration + Governance | :material-clock-outline: | Airflow DAGs, data quality |
| 7 | Observability | :material-clock-outline: | Prometheus, Grafana, alerting |
| 8 | ML Pipeline + Optimization | :material-clock-outline: | Feature store, training, Optuna |
| 9 | ML Serving (3-way) | :material-clock-outline: | BentoML + TorchScript + Triton |
| 10 | CI/CD + Deploy + Polish | :material-clock-outline: | Docker Swarm, K8s, monitoring |

## Phase Details

### Phase 1 — Foundation + Ingestion

**Status**: :material-check-circle: Complete

- [x] Kafka KRaft single-node setup
- [x] MinIO S3-compatible storage
- [x] Binance WebSocket client (trades + klines)
- [x] Kafka producer (confluent-kafka)
- [x] Lake writer (Kafka → MinIO Parquet)
- [x] Pydantic v2 config and models
- [x] Structured logging (JSON + console)
- [x] Async retry decorator
- [x] Unit + integration tests (testcontainers)

### Phase 2 — Batch + Lake Maturation

**Status**: :material-check-circle: Complete

- [x] Binance REST API client (httpx async)
- [x] Historical backfill (chunked, rate-limited)
- [x] PySpark bronze → silver transformer
- [x] Deduplication, type casting, partitioning
- [x] Hive-style partitioning (symbol/interval/year/month)
- [x] End-to-end tests

### Phase 3 — OLAP + dbt

**Status**: :material-check-circle: Complete

- [x] ClickHouse infrastructure (Docker, config, users)
- [x] OLAP loader (MinIO silver → ClickHouse)
- [x] ReplacingMergeTree for idempotent loads
- [x] dbt project initialization
- [x] dbt staging model (`stg_crypto__klines`)
- [x] dbt gold marts (`fct_daily_klines`, `fct_hourly_klines`, `fct_kline_returns`)
- [x] Custom `generate_schema_name` macro
- [x] Unit + integration tests for OLAP loader

### Phase 4 — Stream Processing

**Status**: :material-check-circle: Complete

- [x] PySpark Structured Streaming
- [x] OHLCV filter-and-cast pipeline (closed kline bars → Delta + Kafka)
- [x] VWAP tumbling-window aggregation with event-time watermarking (VWAP, OFI, volatility, trade count)
- [x] Stateful deduplication via `dropDuplicatesWithinWatermark`
- [x] Dual-sink output (Kafka topics + Delta Lake on MinIO)

### Phase 5 — Semantic Layer + BI

**Status**: :material-clock-outline: Planned

- [ ] Metrics store (dimension + metric definitions)
- [ ] Superset / Metabase integration
- [ ] Pre-built dashboards
- [ ] REST API for metrics queries

### Phase 6 — Orchestration + Governance

**Status**: :material-clock-outline: Planned

- [ ] Apache Airflow DAGs
- [ ] Pipeline scheduling and dependency management
- [ ] Data quality checks (Great Expectations)
- [ ] Data catalog and lineage tracking

### Phase 7 — Observability

**Status**: :material-clock-outline: Planned

- [ ] Prometheus metrics export
- [ ] Grafana dashboards
- [ ] Alerting rules (latency, errors, lag)
- [ ] Distributed tracing

### Phase 8 — ML Pipeline + Optimization

**Status**: :material-clock-outline: Planned

- [ ] Feature store (time-series features)
- [ ] PyTorch LSTM model
- [ ] Optuna hyperparameter optimization
- [ ] MLflow experiment tracking
- [ ] GPU training (RTX 3050 Ti)

### Phase 9 — ML Serving (3-way)

**Status**: :material-clock-outline: Planned

- [ ] BentoML service
- [ ] TorchScript export
- [ ] NVIDIA Triton Inference Server
- [ ] A/B testing framework
- [ ] Model performance monitoring

### Phase 10 — CI/CD + Deploy + Polish

**Status**: :material-clock-outline: Planned

- [ ] GitHub Actions CI/CD
- [ ] Docker Swarm deployment
- [ ] Kubernetes manifests
- [ ] Production monitoring
- [ ] Documentation finalization
