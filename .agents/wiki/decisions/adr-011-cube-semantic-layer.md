# ADR-011: Cube.js as the Semantic Layer

## Status

Accepted

## Context

Phase 5 requires establishing a centralized semantic metrics layer on top of our OLAP serving database (ClickHouse). The semantic layer must support defining dimensions, metrics, and pre-aggregations exactly once and exposing them consistently to downstream BI tools (Tableau), custom client APIs, and quantitative analytical scripts.

We evaluated Cube (formerly Cube.js) as the primary option due to its official ClickHouse connector, first-class YAML modeling support, and robust in-memory pre-aggregation layer (Cube Store) backed by Apache Arrow/DataFusion/Parquet.

## Decision

We deploy **Cube** (`cubejs/cube:v0.36`) as a single Docker Compose container in development mode (`CUBEJS_DEV_MODE=true`), utilizing the embedded, in-process Cube Store to minimize memory usage within our ~7-8 GB RAM budget.

We structure the Cube configuration and models into a strict separation of private cubes and public views:
- **Cubes**: Placed in `cube/model/cubes/crypto/` with `public: false` to represent raw physical structures of `gold.fct_daily_klines`, `gold.fct_hourly_klines`, and `gold.fct_kline_returns` ClickHouse tables.
- **Views**: Placed in `cube/model/views/crypto/` to act as the public API contracts exposed to Tableau and downstream queries, grouping relevant business measures and dimensions.
- **Pre-aggregations**: Rollups are configured for each core entity (daily, hourly, returns) to cache aggregates in Cube Store with explicit indexes on `symbol` (and `interval` where applicable) as required by the ClickHouse driver.

## Rationale

1. **Governance & Metric Consistency**: Rather than writing custom queries in Tableau and separate queries in quantitative scripts, both draw from the unified view definitions (`ohlcv_daily`, `ohlcv_hourly`, `price_analytics`).
2. **Resource Constraints**: Embedding Cube Store in-process keeps container memory limited to under 500MB during idle. Decoupled distributed configurations (refresh-workers, standalone Cube Store routers and workers) are deferred to production Swarm/K8s manifests.
3. **ClickHouse Index Enforcement**: ClickHouse pre-aggregations require explicit index definitions in the YAML schema; omitting them triggers compile-time failures.

## Consequences

- Created `cube/` workspace directory mapping configs and models into the container.
- Added `platform-cube` container service to `docker-compose.yml` with port 4000 (Playground/REST) and 15432 (SQL API) exposed.
- Enabled pre-aggregations refreshing on a default 1-hour interval.
- Added a Python-driven BI exporter (`src/semantic/`) using `pandas` and `clickhouse-connect` to extract gold tables to local CSVs for sanity verification and offline Tableau Public uploads.
