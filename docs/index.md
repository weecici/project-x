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

    subgraph LAKE["Lake Storage (Medallion)"]
        S3["MinIO S3\nBronze → Silver → Gold\n(Parquet)"]
    end

    subgraph FUTURE["Future Phases"]
        FLINK["Flink\n(stream processing)"]
        DBT["dbt\n(silver → gold)"]
        CH["ClickHouse\n(OLAP)"]
        ML["ML Pipeline\n(PyTorch + MLflow)"]
    end

    WS --> PROD --> KAFKA
    KAFKA --> S3
    REST --> BACKFILL --> S3
    S3 --> SPARK --> S3

    KAFKA -.-> FLINK
    S3 -.-> DBT -.-> CH
    S3 -.-> ML

    style FUTURE fill:#f0f0f0,stroke:#999,stroke-dasharray: 5 5
```

## Current Status

| Phase | Name | Status |
|:-----:|------|:------:|
| 1 | Foundation + Ingestion | :material-check-circle:{ style="color: green" } Complete |
| 2 | Batch + Lake Maturation | :material-check-circle:{ style="color: green" } Complete |
| 3 | OLAP + dbt | :material-circle-outline: Planned |
| 4 | Stream Processing | :material-circle-outline: Planned |
| 5 | Semantic Layer + BI | :material-circle-outline: Planned |
| 6 | Orchestration + Governance | :material-circle-outline: Planned |
| 7 | Observability | :material-circle-outline: Planned |
| 8 | ML Pipeline + Optimization | :material-circle-outline: Planned |
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

**Python 3.13** · **uv** · **Docker** · **Apache Kafka** · **MinIO** · **PySpark**
