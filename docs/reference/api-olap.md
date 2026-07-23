# API — OLAP Services

OLAP loader and BI exporter: MinIO silver Parquet → ClickHouse, Cube.js → CSV / Google Sheets.

## Overview

The `olap` package handles loading silver-layer data into ClickHouse for OLAP queries and exporting semantic-layer data from Cube.js:

- **Config** — ClickHouse connection, MinIO silver source, and BI exporter settings
- **Loader** — Reads Hive-partitioned Parquet from MinIO, bulk-inserts into ClickHouse via Arrow (DDL inlined)
- **Exporter** — Fetches from Cube.js REST API, saves local CSV, syncs to Google Sheets

::: olap.config
    options:
      show_source: true
      members_order: source

::: olap.loader
    options:
      show_source: true
      members_order: source

::: olap.exporter
    options:
      show_source: true
      members_order: source
