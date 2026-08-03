# Roadmap

10-phase build plan from ingestion to production deployment.

## Phase Overview

| Phase | Name | Status | Description |
|:-----:|------|:------:|-------------|
| 1 | Foundation + Ingestion | :material-check-circle:{ style="color: green" } | Live WS → Kafka → MinIO lake |
| 2 | Batch + Lake Maturation | :material-check-circle:{ style="color: green" } | REST backfill, PySpark silver |
| 3 | OLAP + dbt | :material-check-circle:{ style="color: green" } | ClickHouse, dbt staging + gold marts |
| 4 | Stream Processing | :material-check-circle:{ style="color: green" } | PySpark Structured Streaming |
| 5 | Semantic Layer + BI | :material-check-circle:{ style="color: green" } | Cube.js metrics API, BI exporter, Tableau |
| 6 | Orchestration + Governance | :material-check-circle:{ style="color: green" } | Airflow DAGs, lineage compiler, OpenLineage |
| 7 | Observability | :material-check-circle:{ style="color: green" } | Prometheus, Grafana, Loki, Alloy, alerting |
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

**Status**: :material-check-circle: Complete

- [x] Cube.js semantic layer (3 cubes on ClickHouse gold tables)
- [x] Cube public views (`ohlcv_daily`, `ohlcv_hourly`, `price_analytics`)
- [x] Pre-aggregations for query performance
- [x] BI exporter CLI (`export-bi`): Cube REST API → local CSV + Google Sheets
- [x] `BiExporterConfig` + `GoogleServiceAccountConfig`
- [x] Tableau connection (PostgreSQL wire protocol to Cube SQL API)
- [x] Unit + integration tests for exporter

### Phase 6 — Orchestration + Governance

**Status**: :material-check-circle: Complete

- [x] Apache Airflow 3.3.0 (LocalExecutor) in Docker
- [x] PostgreSQL metadata database (Alpine, 256MB limit)
- [x] Airflow Dockerfile (Java 17 + uv + editable install)
- [x] `crypto_batch_backfill` DAG — Dynamic Task Mapping, parallel per-symbol backfill
- [x] `crypto_olap_serving` DAG — ClickHouse load → dbt → tests → BI export + lineage
- [x] `crypto_ml_retrain` DAG — placeholder for Phase 8 ML retraining
- [x] Lineage Manifest Compiler (5-source dynamic extraction)
- [x] OpenLineage v1.0 + OpenMetadata compatible JSON manifest
- [x] `export-lineage` CLI entry point
- [x] `OrchestrationConfig` + `GovernanceConfig` (Pydantic Settings)
- [x] Unit tests (DAG validation, config, lineage manifest)
- [x] Integration test (end-to-end manifest export)

### Phase 7 — Observability

**Status**: :material-check-circle: Complete

- [x] Prometheus v3.4.2 (15s scrape interval, 7 scrape jobs)
- [x] Grafana 12.0.0 (dashboard-as-code, `allowUiUpdates: false`)
- [x] Grafana Alloy v1.9.1 (replaces EOL Promtail, Docker log collection)
- [x] Loki 3.5.0 (filesystem TSDB, schema v13, 7-day retention)
- [x] AlertManager v0.28.1 (symptom-based alerting rules)
- [x] kafka-exporter (consumer lag + partition offset metrics)
- [x] cAdvisor (per-container CPU/RAM/Net/IO metrics)
- [x] node-exporter (host hardware metrics)
- [x] statsd-exporter (Airflow StatsD → Prometheus bridge)
- [x] ClickHouse native Prometheus endpoint (port 9363)
- [x] Airflow StatsD emission enabled
- [x] Platform Infrastructure Health dashboard (4 panels)
- [x] Host Hardware & Node Metrics dashboard (2 panels)
- [x] ML Model Serving Benchmarks stub dashboard (2 panels, Phase 9)
- [x] Prometheus alert rules: KafkaConsumerLagHigh, ClickHouseQueryThreadsHigh, ContainerMemoryHigh, HostHighCpuLoad, HostDiskSpaceLow
- [x] Prometheus recording rules (5 rules for dashboard queries)
- [x] Grafana provisioning: datasources (Prometheus, Loki, AlertManager), dashboards, alerting contact points
- [x] AlertManager routing + dedup config
- [x] Docker Compose profile `observability` (~912 MB total)
- [x] `just obs-up`, `just obs-down`, `just obs-reload-prometheus` recipes
- [x] Unit tests (Prometheus, Loki, Grafana provisioning configs)

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
