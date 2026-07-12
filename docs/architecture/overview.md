# Architecture Overview

The Crypto Platform follows a **Medallion Architecture** (Bronze → Silver → Gold) with a hybrid streaming + batch design and OLAP analytics via ClickHouse.

## High-Level Architecture

```mermaid
flowchart TB
    subgraph SOURCES["Data Sources"]
        WS["Binance WebSocket\nLive trades, klines"]
        REST["Binance REST\nHistorical klines"]
    end

    subgraph INGESTION["Ingestion Layer (Phase 1)"]
        PROD["ws_client.py\nconfluent-kafka producer"]
        KAFKA["Apache Kafka\nKRaft mode, 3 topics"]
    end

    subgraph LAKE["Lake Storage (Phase 1–2)"]
        S3["MinIO S3\nS3-compatible object store"]
        BRONZE["Bronze Layer\nRaw data, Hive-partitioned Parquet"]
        SILVER["Silver Layer\nDeduplicated, typed, partitioned"]
    end

    subgraph BATCH["Batch Processing (Phase 2)"]
        BACKFILL["binance_rest.py\nhttpx async, rate-limited"]
        PYSPARK["PySpark\nbronze → silver transformer"]
    end

    subgraph OLAP["OLAP Layer (Phase 3)"]
        LOADER["olap/loader.py\nMinIO → ClickHouse"]
        CH["ClickHouse\nReplacingMergeTree"]
        DBT["dbt models\nstaging → gold marts"]
    end

    subgraph FUTURE["Future Phases"]
        FLINK["Flink\nWindowed aggregations"]
        SEMANTIC["Semantic Layer\nMetrics + dimensions"]
        ML["ML Pipeline\nFeature store + model serving"]
    end

    WS --> PROD --> KAFKA
    REST --> BACKFILL

    KAFKA --> S3
    BACKFILL --> S3
    S3 --- BRONZE
    PYSPARK --> S3
    BRONZE --> PYSPARK
    PYSPARK --> SILVER

    SILVER --> LOADER --> CH
    CH --> DBT

    DBT -.-> FLINK
    DBT -.-> SEMANTIC
    DBT -.-> ML

    style FUTURE fill:#f5f5f5,stroke:#999,stroke-dasharray: 5 5
```

## Medallion Architecture

The platform organizes data in three tiers:

| Layer | Format | Purpose | Partitioning |
|-------|--------|---------|--------------|
| **Bronze** | Parquet | Raw data exactly as received | `symbol/`, `topic/`, `year/`, `month/`, `day/` |
| **Silver** | Parquet + ClickHouse | Deduplicated, typed, cleaned | `symbol/`, `interval/`, `year/`, `month/` |
| **Gold** | ClickHouse tables | Aggregated, analytics-ready | `symbol/`, `trade_date` / `hour_at` |

### Bronze Layer

- Written directly from the Kafka consumer and REST backfiller
- **No transformations** — data is preserved exactly as received
- Each message gets a UUID (`message_id`), Kafka offset tracking, and ingestion timestamp
- Files flushed every 30 seconds or 1,000 messages (whichever comes first)
- UUID-based filenames prevent duplicate writes on retry

### Silver Layer

- **Parquet**: Written by PySpark batch jobs reading from bronze
  - **Deduplication** by `symbol + open_time` (klines) or `symbol + trade_id` (trades)
  - Type casting (strings → decimals, timestamps → longs)
  - Partitioned by `symbol/interval/year/month` for optimal query patterns
  - Overwrites partitions atomically
- **ClickHouse**: Loaded by `olap/loader.py` from MinIO silver Parquet
  - `ReplacingMergeTree(_loaded_at)` engine for idempotent loads
  - Partitioned by `(symbol, toYYYYMM(open_time))`
  - Ordered by `(symbol, interval, open_time)`

### Gold Layer (Phase 3 — In Progress)

- dbt models consuming ClickHouse silver data
- **Staging**: `stg_crypto__klines` — typed view with surrogate keys
- **Marts**:
  - `fct_daily_klines` — daily OHLCV aggregates
  - `fct_hourly_klines` — hourly OHLCV aggregates
  - `fct_kline_returns` — log returns per bar
- Query-optimized tables for OLAP and downstream consumption

## Design Principles

1. **Append-only lake** — No in-place mutations. All writes are new Parquet files.
2. **Idempotent writes** — UUID-based dedup means re-running a job produces the same result.
3. **Schema evolution** — Parquet + PySpark handles schema changes gracefully.
4. **Decoupled components** — Kafka decouples producers from consumers. Each component can be restarted independently.
5. **Local-first development** — Everything runs on a single machine with Docker. No cloud dependencies for development.
6. **Medallion consistency** — Bronze → Silver → Gold naming maps directly to MinIO buckets and ClickHouse databases.

## Component Map

| Component | Technology | Location | Purpose |
|-----------|-----------|----------|---------|
| WebSocket client | `confluent-kafka` + `websockets` | `src/ingestion/producer/ws_client.py` | Live Binance data stream |
| Kafka producer | `confluent-kafka` | `src/ingestion/producer/ws_client.py` | Publish to Kafka topics |
| Lake writer | `confluent-kafka` + `pyarrow` | `src/ingestion/writer/lake_writer.py` | Kafka → bronze Parquet |
| REST backfiller | `httpx` + `pyarrow` | `src/batch/backfill/binance_rest.py` | Historical data → bronze |
| Silver transformer | PySpark | `src/batch/silver/kline_transformer.py` | Bronze → silver dedup |
| OLAP loader | `clickhouse-connect` + `pyarrow` | `src/olap/loader.py` | MinIO silver → ClickHouse |
| dbt models | `dbt-clickhouse` | `dbt/models/` | Silver → gold SQL transforms |
| Config | Pydantic v2 | `src/ingestion/config.py`, `src/batch/config.py`, `src/olap/config.py` | Typed, validated settings |
| Shared utils | Various | `src/utils/` | Logging, retry, S3 access |

## Infrastructure

| Service | Port | Purpose |
|---------|------|---------|
| Apache Kafka | 9094 | Message broker (KRaft mode, no ZooKeeper) |
| Kafka UI | 8080 | Web UI for topic inspection |
| MinIO API | 9000 | S3-compatible object storage API |
| MinIO Console | 9001 | Web UI for bucket browsing |
| ClickHouse HTTP | 8123 | ClickHouse HTTP interface (OLAP queries, dbt) |
| ClickHouse TCP | 9009 | ClickHouse native TCP (internal replication) |
