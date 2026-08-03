# ADR-013: Prometheus, Grafana, Loki, and Grafana Alloy for Observability

## Status

Accepted

## Context

Phase 7 requires adding comprehensive full-stack observability (metrics, log aggregation, and alerting) across all platform components (Kafka, MinIO, PySpark, ClickHouse, Airflow, and model serving endpoints). The machine spec imposes a hard usable RAM ceiling (~7–8 GB usable), requiring a low-footprint architecture that avoids out-of-memory crashes while adhering to modern DevOps standards.

## Decision

1. **Metrics Collection & Storage**:
   - Standardize on **Prometheus v3.4.2** (`prom/prometheus:v3.4.2`) scraping metrics on a 15-second evaluation interval.
   - Use **kafka-exporter** (`danielqsj/kafka-exporter`) to track consumer group lag and partition offsets externally via the Kafka Admin API.
   - Enable **ClickHouse native Prometheus endpoint** on port `9363` (`custom-config.xml`), avoiding third-party sidecars.
   - Enable **Airflow StatsD emission** bridged via `statsd-exporter` (`prom/statsd-exporter`).
   - Scrape **MinIO cluster metrics** at `/minio/v2/metrics/cluster`.
   - Deploy **cAdvisor** (`gcr.io/cadvisor/cadvisor`) for per-container CPU/RAM/Net/IO metrics and **node-exporter** for host system hardware metrics.

2. **Log Aggregation**:
   - Adopt **Grafana Loki 3.5.0** with filesystem TSDB storage (schema v13) and 168-hour (7-day) retention policy.
   - Deploy **Grafana Alloy v1.9.1** (`grafana/alloy`) as the unified log collector agent reading `/var/run/docker.sock`. **Promtail was deprecated/EOL as of March 2, 2026**; Alloy is the official successor.

3. **Dashboards as Code & Alerting**:
   - Use **Grafana 12.0.0** with automated provisioning configuration (`allowUiUpdates: false`) so all dashboards are stored as committed JSON files in Git (`infra/observability/grafana/dashboards/`).
   - Pre-wire an **ML Serving Benchmark Dashboard** stub (`ml_serving_stub.json`) measuring throughput (rps) and p50/p95/p99 latency for Triton, BentoML, and FastAPI backends ahead of Phase 9.
   - Deploy **AlertManager v0.28.1** with structured alerting rules (`platform.yml`, `infra.yml`) prioritizing user-facing symptoms over raw CPU blips.

4. **Resource Management**:
   - All observability services are placed under **Docker Compose profile `observability`** (`docker compose --profile observability up -d`).
   - This keeps the base platform runnable within the 7–8 GB usable RAM constraint while making full observability available on demand. Total observability stack memory limit is capped at ~1 GB.

## Rationale

1. **Industry Alignment**: Using Grafana Alloy over EOL Promtail, ClickHouse native metrics over legacy sidecars, and kafka-exporter for lag metrics reflects current production best practices.
2. **Dashboard-as-Code Contract**: Setting `allowUiUpdates: false` prevents configuration drift between the Grafana UI and Git repository.
3. **Strict Memory Budget**: Profile-based execution allows running full observability without starving the primary streaming and lakehouse services.
