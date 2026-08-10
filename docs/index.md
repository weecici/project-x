---
hide:
  - navigation
---

# Crypto Platform

**End-to-end crypto market intelligence platform** — live + historical data ingestion, lakehouse storage, OLAP analytics, and ML price-movement prediction.

<div class="grid cards" markdown>

-   :material-rocket-launch-outline:{ .lg .middle } **Getting Started**

    ---

    Get up and running in minutes.

    [:octicons-arrow-right-24: Prerequisites](getting-started/prerequisites.md)

-   :material-sitemap:{ .lg .middle } **Architecture**

    ---

    Understand the system design and data flow.

    [:octicons-arrow-right-24: Overview](architecture/overview.md)

-   :material-console:{ .lg .middle } **Quick Start**

    ---

    Run the full pipeline locally.

    [:octicons-arrow-right-24: Quick Start](getting-started/quickstart.md)

-   :material-road-variant:{ .lg .middle } **Roadmap**

    ---

    10-phase build plan from ingestion to production deploy.

    [:octicons-arrow-right-24: Roadmap](development/roadmap.md)

</div>

## Architecture Overview

```mermaid
flowchart TB
    subgraph SRC["Data Sources"]
        WS["Binance WebSocket\n(live trades/klines)"]
        REST["Binance REST\n(historical klines)"]
    end

    subgraph INGEST["Ingestion"]
        PROD["Python producer\n(websocket → Kafka)"]
        KAFKA["Apache Kafka\n(topics: trades, klines)"]
    end

    subgraph BATCH["Batch Processing"]
        BACKFILL["REST backfill\n(historical → bronze)"]
        SPARK["PySpark silver\n(dedup, cast, partition)"]
    end

    subgraph STREAM["Stream Processing"]
        SPARK_STREAM["PySpark Structured Streaming\n(OHLCV, VWAP, OFI, volatility)"]
    end

    subgraph LAKE["Lake Storage (Medallion)"]
        S3["MinIO S3\nBronze → Silver\n(Parquet + Delta)"]
    end

    subgraph OLAP["OLAP + Analytics"]
        LOADER["OLAP loader\n(silver → ClickHouse)"]
        CH["ClickHouse\n(OLAP database)"]
        DBT["dbt models\n(staging → gold marts)"]
        CUBE["Cube.js\n(semantic layer)"]
    end

    subgraph ORCH["Orchestration"]
        AIRFLOW["Airflow\n(LocalExecutor)"]
        LINEAGE["Lineage compiler\n(OpenMetadata)"]
    end

    subgraph OBS["Observability"]
        PROM["Prometheus\n(metrics)"]
        GRAF["Grafana\n(dashboards)"]
        LOKI["Loki\n(logs)"]
        ALERT["AlertManager\n(alerts)"]
    end

    subgraph ML["ML Pipeline (Phase 8)"]
        FEAT["Feature Engineering\n(PySpark + Numba JIT)"]
        TRAIN["CryptoLSTM Training\n(PyTorch + MLflow)"]
        OPT["Optimization\n(pruning, quantization, ONNX)"]
    end

    WS --> PROD --> KAFKA
    KAFKA --> S3
    KAFKA --> SPARK_STREAM
    REST --> BACKFILL --> S3
    S3 --> SPARK --> S3
    SPARK_STREAM --> S3

    S3 --> LOADER --> CH
    CH --> DBT
    DBT --> CUBE

    CUBE -.-> AIRFLOW
    AIRFLOW --> LINEAGE

    DBT --> FEAT
    FEAT --> TRAIN
    TRAIN --> OPT

    PROM -.-> GRAF
    LOKI -.-> GRAF

    style ORCH fill:#fff3e0,stroke:#e65100,stroke-dasharray: 5 5
    style OBS fill:#e8f5e9,stroke:#2e7d32,stroke-dasharray: 5 5
```

## Current Status

| Phase | Name | Status |
|:-----:|------|:------:|
| 1 | Foundation + Ingestion | :material-check-circle:{ style="color: green" } Complete |
| 2 | Batch + Lake Maturation | :material-check-circle:{ style="color: green" } Complete |
| 3 | OLAP + dbt | :material-check-circle:{ style="color: green" } Complete |
| 4 | Stream Processing | :material-check-circle:{ style="color: green" } Complete |
| 5 | Semantic Layer + BI | :material-check-circle:{ style="color: green" } Complete |
| 6 | Orchestration + Governance | :material-check-circle:{ style="color: green" } Complete |
| 7 | Observability | :material-check-circle:{ style="color: green" } Complete |
| 8 | ML Pipeline + Optimization | :material-check-circle:{ style="color: green" } Complete |
| 9 | ML Serving (3-way) | :material-circle-outline: Planned |
| 10 | CI/CD + Deploy + Polish | :material-circle-outline: Planned |

## Quick Install

```bash
git clone https://github.com/yourusername/crypto-platform.git
cd crypto-platform
uv sync
```

[:octicons-arrow-right-24: Full Installation Guide](getting-started/installation.md)

---

**Python 3.13** · **uv** · **Docker** · **Apache Kafka** · **MinIO** · **PySpark** · **ClickHouse** · **dbt** · **Cube.js** · **Airflow**
