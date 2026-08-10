# dbt Models

Transform silver-layer data into analytics-ready gold tables using dbt and ClickHouse.

## Overview

The dbt project consumes data from the ClickHouse `silver` database (loaded by the OLAP loader) and produces aggregated gold-layer tables. It follows the medallion architecture:

```
MinIO silver Parquet
    ↓  (OLAP loader)
ClickHouse silver.klines_raw   (ReplacingMergeTree)
    ↓  (dbt staging)
silver.stg_crypto__klines      (view — typed, deduplicated)
    ↓  (dbt marts)
gold.fct_daily_klines          (table — daily OHLCV)
gold.fct_hourly_klines         (table — hourly OHLCV)
gold.fct_kline_returns         (table — log returns)
```

## Project Structure

```
dbt/
├── dbt_project.yml             # Project config, materializations
├── profiles.yml                # ClickHouse connection
├── packages.yml                # dbt_utils dependency
├── macros/
│   └── generate_schema_name.sql  # Schema naming override
└── models/
    ├── staging/crypto/
    │   ├── sources.yml         # Source: silver.klines_raw
    │   ├── staging.yml         # Schema: stg_crypto__klines
    │   └── stg_crypto__klines.sql
    └── marts/
        ├── marts.yml           # Schemas: fct_daily/hourly/returns
        ├── fct_daily_klines.sql
        ├── fct_hourly_klines.sql
        └── fct_kline_returns.sql
```

## Connection Profile

Defined in `dbt/profiles.yml`:

| Setting | Value |
|---------|-------|
| Type | `clickhouse` (via `dbt-clickhouse`) |
| Driver | `http` (clickhouse-connect HTTP interface) |
| Target schema | `gold` |
| Host | `{{ env_var('CLICKHOUSE_HOST', 'localhost') }}` |
| Port | `{{ env_var('CLICKHOUSE_PORT', '8123') }}` |
| Threads | `1` (conservative for local dev) |

All connection details are sourced from `CLICKHOUSE_*` environment variables. See [Environment Variables](../reference/env-vars.md) for the full list.

## Running dbt

### Install Dependencies

```bash
just dbt-deps
```

This installs `dbt-labs/dbt_utils` (used for `generate_surrogate_key`).

### Run All Models

```bash
just dbt-run
```

This materializes:

1. **Staging**: `stg_crypto__klines` → view in `silver` database
2. **Marts**: `fct_daily_klines`, `fct_hourly_klines`, `fct_kline_returns` → tables in `gold` database

### Run Tests

```bash
just dbt-test
```

Runs all `not_null`, `unique`, and `expression_is_true` tests defined in the YAML schema files.

### Combined

```bash
just dbt-run && just dbt-test
```

!!! note "Experimental adapter"
    All dbt commands use `DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true` (set automatically in justfile) because `dbt-clickhouse` is marked as experimental.

## Models

### Staging: `stg_crypto__klines`

A typed view on `silver.klines_raw`. Serves as the stable schema contract for all gold models.

| Column | Type | Description |
|--------|------|-------------|
| `kline_id` | `String` | Surrogate key from `(symbol, interval, open_time)` |
| `symbol` | `String` | Trading pair (e.g., `BTCUSDT`) |
| `interval` | `String` | Kline interval (`1m`, `5m`, `1h`, etc.) |
| `open_at` | `DateTime64` | Bar open timestamp (renamed from `open_time`) |
| `close_at` | `DateTime64` | Bar close timestamp (renamed from `close_time`) |
| `open` | `Decimal(18,8)` | Open price |
| `high` | `Decimal(18,8)` | High price |
| `low` | `Decimal(18,8)` | Low price |
| `close` | `Decimal(18,8)` | Close price |
| `volume` | `Decimal(18,8)` | Base asset volume |
| `quote_volume` | `Decimal(18,8)` | Quote asset volume |
| `num_trades` | `UInt32` | Number of trades |
| `taker_buy_base_volume` | `Decimal(18,8)` | Taker buy base volume |
| `taker_buy_quote_volume` | `Decimal(18,8)` | Taker buy quote volume |

Key behaviors:

- Uses `FINAL` keyword to force deduplication from `ReplacingMergeTree`
- Surrogate key generated via `dbt_utils.generate_surrogate_key`

### Mart: `fct_daily_klines`

Daily OHLCV aggregates from 1-minute bars. One row per `(symbol, trade_date)`.

| Column | Type | Description |
|--------|------|-------------|
| `daily_kline_id` | `String` | Surrogate key from `(symbol, trade_date)` |
| `symbol` | `String` | Trading pair |
| `trade_date` | `Date` | UTC date |
| `open` | `Decimal(18,8)` | First bar's open (`argMin`) |
| `high` | `Decimal(18,8)` | Max high across all bars |
| `low` | `Decimal(18,8)` | Min low across all bars |
| `close` | `Decimal(18,8)` | Last bar's close (`argMax`) |
| `volume` | `Decimal(18,8)` | Sum of volume |
| `quote_volume` | `Decimal(18,8)` | Sum of quote volume |
| `num_trades` | `UInt32` | Sum of trade counts |

Tests: `high >= low`, `volume >= 0`, `num_trades >= 0`.

### Mart: `fct_hourly_klines`

Hourly OHLCV aggregates from 1-minute bars. Same logic as daily, grouped by `(symbol, hour_at)`.

### Mart: `fct_kline_returns`

Log returns for every kline bar across all intervals.

| Column | Type | Description |
|--------|------|-------------|
| `kline_return_id` | `String` | Surrogate key from `(symbol, interval, open_at)` |
| `symbol` | `String` | Trading pair |
| `interval` | `String` | Kline interval |
| `open_at` | `DateTime64` | Bar open timestamp |
| `close` | `Decimal(18,8)` | Close price |
| `log_return` | `Float64` | `ln(close / prev_close)`, NULL for first bar |

Uses ClickHouse's `lag()` window function to compute `ln(close / previous_close)`.

## Materializations

| Model | Materialization | Database | Description |
|-------|----------------|----------|-------------|
| `stg_crypto__klines` | `view` | `silver` | Lightweight, always-fresh reference |
| `fct_daily_klines` | `table` | `gold` | Pre-aggregated daily OHLCV |
| `fct_hourly_klines` | `table` | `gold` | Pre-aggregated hourly OHLCV |
| `fct_kline_returns` | `table` | `gold` | Log returns per bar |

## Schema Naming Override

The `macros/generate_schema_name.sql` macro overrides dbt's default behavior. By default, dbt prepends the target schema to custom schemas (e.g., `gold_silver`), creating incorrect database names. This macro resolves directly to the configured custom schema (`silver` or `gold`), matching the medallion architecture.

## ClickHouse-Specific Details

- **ReplacingMergeTree**: The source table `silver.klines_raw` uses `ReplacingMergeTree(_loaded_at)` for idempotent loads. Background merges deduplicate by primary key.
- **`FINAL` keyword**: The staging model uses `FROM {{ source(...) }} FINAL` to force deduplication at query time.
- **Hive partitioning**: Both ClickHouse tables and Parquet files use `(symbol, toYYYYMM(open_time))` partitioning.
- **Memory limits**: ClickHouse is capped at 256 MB per query, 2 threads, 2 GB container limit.
