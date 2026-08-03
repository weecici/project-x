# Wiki Index

Content catalog of all wiki pages. Read this first to find relevant docs, then drill into referenced pages.

## Architecture

| Page | Summary |
|---|---|
| [architecture/overview.md](architecture/overview.md) | Mermaid diagram of the full system: Kafka → Spark Streaming/Batch → S3/MinIO → dbt → ClickHouse/Doris → Cube → dashboards; plus ML pipeline and infra layers |
| [architecture/breakdown.md](architecture/breakdown.md) | Component-by-component explanation of every tool choice (17 sections covering ingestion through CI/CD), with rationale and CV-signal notes |

## Structure

| Page | Summary |
|---|---|
| [structure/phase.md](structure/phase.md) | 10-phase build order table; each phase lists active services and its concrete deliverable checkpoint |
| [structure/project-structure.md](structure/project-structure.md) | Full directory layout with annotations; reflects Phase 1 actual files and future phase stubs |

## Architecture Decision Records

| ADR | Decision |
|---|---|
| [decisions/adr-001-kafka-kraft.md](decisions/adr-001-kafka-kraft.md) | Kafka in KRaft mode (no ZooKeeper) — saves RAM, aligns with Kafka's roadmap |
| [decisions/adr-002-confluent-kafka.md](decisions/adr-002-confluent-kafka.md) | `confluent-kafka` as Python Kafka client — librdkafka-backed, asyncio GA 2026 |
| [decisions/adr-003-phase1-no-flink.md](decisions/adr-003-phase1-no-flink.md) | Flink deferred to Phase 4 — subsequently replaced by Spark Structured Streaming due to Python 3.13 compatibility constraints |
| [decisions/adr-004-gpu-profile.md](decisions/adr-004-gpu-profile.md) | RTX 3050 Ti (4 GB VRAM, CUDA 13.3, sm_86) used in Phases 8–9 for training + serving |
| [decisions/adr-005-src-layout.md](decisions/adr-005-src-layout.md) | `src/` directory layout — prevents accidental imports, conforms to standard Python packaging best practices |
| [decisions/adr-006-utils-package.md](decisions/adr-006-utils-package.md) | `src/utils/` for cross-cutting helpers (logging, retry, storage) — distinct from `core/` which is for domain logic |
| [decisions/adr-007-pyspark-local-mode.md](decisions/adr-007-pyspark-local-mode.md) | PySpark `local[*]` for Phase 2 silver — no cluster overhead; Delta Lake deferred to Phase 4 |
| [decisions/adr-008-httpx-rest-client.md](decisions/adr-008-httpx-rest-client.md) | `httpx` over `requests`/`aiohttp` — native async, first-class `pytest-httpx` mock support |
| [decisions/adr-009-clickhouse-olap.md](decisions/adr-009-clickhouse-olap.md) | ClickHouse over DuckDB for OLAP — server-mode, Cube.js-compatible, ReplacingMergeTree for idempotent loads |
| [decisions/adr-010-pyspark-structured-streaming.md](decisions/adr-010-pyspark-structured-streaming.md) | PySpark Structured Streaming as Phase 4 Stream Processing Engine due to Python 3.13 compatibility constraints |
| [decisions/adr-011-cube-semantic-layer.md](decisions/adr-011-cube-semantic-layer.md) | Cube.js as the Semantic Layer — role-split, embedded/in-process Cube Store, and ClickHouse index rules |
| [decisions/adr-012-orchestration-governance.md](decisions/adr-012-orchestration-governance.md) | Apache Airflow & OpenMetadata for Orchestration and Governance — LocalExecutor, OpenLineage standard, and manifest compiler |
| [decisions/adr-013-observability.md](decisions/adr-013-observability.md) | Prometheus, Grafana, Loki, and Grafana Alloy for Observability — Grafana Alloy log collection, dashboard-as-code, and profile execution |
