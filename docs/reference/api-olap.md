# API — OLAP

OLAP loading pipeline: MinIO silver Parquet → ClickHouse.

## Overview

The `olap` package handles loading silver-layer data into ClickHouse for OLAP queries:

- **Config** — ClickHouse connection and MinIO silver source settings
- **Loader** — Reads Hive-partitioned Parquet from MinIO, bulk-inserts into ClickHouse via Arrow
- **Schema** — ClickHouse DDL for the `klines_raw` table (ReplacingMergeTree)

::: olap.config
    options:
      show_source: true
      members_order: source

::: olap.loader
    options:
      show_source: true
      members_order: source

::: olap.schema
    options:
      show_source: true
      members_order: source
