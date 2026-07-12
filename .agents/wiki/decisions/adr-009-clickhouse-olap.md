# ADR-009: ClickHouse as the OLAP Engine

## Status

Accepted

## Context

Phase 3 requires a queryable OLAP layer on top of the silver Parquet data. The engine
must support dbt (for gold models), serve Phase 5 Cube.js, and handle analytical queries
over months of 1-minute kline bars efficiently. Two candidates were evaluated: ClickHouse
and DuckDB.

## Decision

Use **ClickHouse** (`clickhouse/clickhouse-server:25-alpine`) as the OLAP engine, separating databases into `silver` (for raw ingested tables and staging views) and `gold` (for downstream analytical tables) to enforce clean medallion layer boundaries. Clients access ClickHouse via the `clickhouse-connect` HTTP interface.

## Comparison

| Criterion | ClickHouse ✅ | DuckDB |
|---|---|---|
| Server-mode deployment | ✅ Docker container | ❌ Embedded only |
| Queryable from multiple clients simultaneously | ✅ | ❌ |
| dbt adapter | ✅ `dbt-clickhouse` | ✅ `dbt-duckdb` |
| Phase 5 Cube.js integration | ✅ Official connector | ⚠️ Limited / unofficial |
| Phase 5 Tableau Public | ✅ JDBC/ODBC drivers | ⚠️ Limited |
| Memory footprint | ~300–400 MB + 512 MB cap | ~50 MB |
| Column-store OLAP performance | ✅ Purpose-built | ✅ |
| Requires server process on host | ✅ Docker | ❌ |

DuckDB's embedded nature is ideal for ad-hoc analytics scripts but blocks the
multi-client architecture needed for Cube.js + dashboards in Phase 5. ClickHouse
was always the correct choice per the 10-phase plan; this ADR confirms it.

## Table Design

`silver.klines_raw` is the entry point for historical and streaming silver Parquet files, using `ReplacingMergeTree`:
- **Engine**: deduplicates by `ORDER BY (symbol, interval, open_time)` on background merge, keeping the latest loaded record.
- **Idempotency**: re-running the OLAP loader produces the same final state.
- **Partition**: `(symbol, toYYYYMM(open_time))` — enables partition pruning for symbol and date range queries.
- **Types**: `LowCardinality(String)` for symbols/intervals (dictionary-encoded); `Decimal(18, 8)` for prices/volumes; `DateTime64(3, 'UTC')` for timestamps.

## Python Client

`clickhouse-connect` (HTTP driver) over `clickhouse-driver` (native TCP):
- HTTP interface is the recommended path for Python 3.13+ applications per ClickHouse docs.
- `client.insert_arrow()` sends PyArrow IPC directly — zero-copy columnar transfer.
- dbt-clickhouse also defaults to HTTP.

## dbt Schema Strategy

To enforce clean architectural boundaries:
- **Silver Database**: Holds staging models (e.g., `silver.stg_crypto__klines` view).
- **Gold Database**: Holds final fact tables (e.g., `gold.fct_daily_klines` table).
The `generate_schema_name` macro overrides dbt's default behavior, mapping custom schema tags (`silver` or `gold`) directly to flat, separate databases in ClickHouse without default prefixes.

## Consequences

- `clickhouse-connect>=0.8.0` added to dependencies.
- `testcontainers[clickhouse]` added to dev dependencies.
- `docker-compose.yaml` adds a `clickhouse` service with `mem_limit: 2048m` to prevent OOM errors on multi-core environments when executing parallelized dbt tests.
- Replaced file-based container initialization with code-driven initialization: `src/olap/schema.py` contains `KLINES_RAW_DDL`, and `src/olap/loader.py` programmatically creates the `silver` and `gold` databases and the raw table on startup.
- dbt project lives in `dbt/` with `profiles.yml` utilizing HTTP connection parameters, capped at `threads: 1` in development to ensure stable memory profiles.
