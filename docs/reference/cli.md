# CLI Commands

All commands are defined as `[project.scripts]` in `pyproject.toml` and run via `uv run`. Shortcut recipes are available in the `justfile`.

## `produce`

Start the live Binance WebSocket → Kafka producer.

```bash
uv run produce
```

| Behavior | Detail |
|----------|--------|
| **Input** | Binance WebSocket (live trades + klines) |
| **Output** | Kafka topics `raw.trades`, `raw.klines` |
| **Shutdown** | `Ctrl+C` (graceful, flushes pending) |
| **Config** | `INGESTION_*` env vars |

**What it does:**

1. Connects to Binance WebSocket streams for configured symbols/intervals
2. Parses each message into a typed Pydantic model (`Trade` or `Kline`)
3. Partitions messages by symbol and publishes to the correct Kafka partition
4. Handles reconnection automatically on disconnects
5. Logs message counts per symbol at INFO level

---

## `write-lake`

Start the Kafka → MinIO bronze lake writer.

```bash
uv run write-lake
```

| Behavior | Detail |
|----------|--------|
| **Input** | Kafka topics `raw.trades`, `raw.klines` |
| **Output** | MinIO `bronze/` bucket (Hive-partitioned Parquet) |
| **Shutdown** | `Ctrl+C` (flushes remaining messages) |
| **Config** | `INGESTION_*` env vars |

**What it does:**

1. Starts consuming from all partitions of `raw.trades` and `raw.klines`
2. Buffers messages in memory
3. Flushes to MinIO when either threshold is met:
   - 30 seconds since last flush (`INGESTION_FLUSH_INTERVAL_SECONDS`)
   - 1,000 messages buffered (`INGESTION_FLUSH_THRESHOLD`)
4. Each flush writes a new Parquet file with UUID-based filename
5. On shutdown, flushes all remaining buffered messages

**File paths:**

```
bronze/trades/symbol=BTCUSDT/year=2026/month=07/day=09/<uuid>.parquet
bronze/klines/symbol=BTCUSDT/interval=1m/year=2026/month=07/day=09/<uuid>.parquet
```

---

## `backfill`

Backfill historical kline data from Binance REST API.

```bash
uv run backfill
```

| Behavior | Detail |
|----------|--------|
| **Input** | Binance REST API (`/api/v3/klines`) |
| **Output** | MinIO `bronze/` bucket (Parquet files) |
| **Config** | `BACKFILL_*` env vars |

**What it does:**

1. Splits the date range into configurable chunks (default: 7 days)
2. Fetches 1000 candles per API request (configurable via `BACKFILL_LIMIT`)
3. Applies rate limiting (10 tokens/request, 1200 tokens/minute)
4. Retries failed chunks up to 3 times with exponential backoff
5. Writes each chunk as a Parquet file to bronze
6. Reports `BackfillResult` with success/failure counts

**Example:**

```bash
BACKFILL_SYMBOLS='["BTCUSDT"]' \
BACKFILL_START_TIME=2026-01-01T00:00:00Z \
BACKFILL_CHUNK_DAYS=14 \
uv run backfill
```

---

## `silver`

Run the PySpark bronze → silver transformation.

```bash
uv run silver
```

| Behavior | Detail |
|----------|--------|
| **Input** | MinIO `bronze/klines/**/*.parquet` |
| **Output** | MinIO `silver/klines/` (overwritten) |
| **Config** | `SILVER_*` env vars |

**What it does:**

1. Reads all bronze kline Parquet files via PySpark
2. Deduplicates by `symbol + open_time` (keeps latest)
3. Casts types (strings → decimals, timestamps → longs)
4. Repartitions (200 partitions) for write performance
5. Overwrites silver layer atomically (Hive-style)
6. Reports `SilverResult` with input/output/duplicate counts

**When to run:**

- After new bronze data arrives (manual or scheduled)
- The transformation is idempotent — re-running produces the same result

---

## `load-olap`

Load silver-layer Parquet from MinIO into ClickHouse.

```bash
uv run load-olap
```

| Behavior | Detail |
|----------|--------|
| **Input** | MinIO `silver/klines/**/*.parquet` (Hive-partitioned) |
| **Output** | ClickHouse `silver.klines_raw` (ReplacingMergeTree) |
| **Config** | `CLICKHOUSE_*` + `MINIO_*` env vars |

**What it does:**

1. Creates `silver` and `gold` databases if they don't exist
2. Runs DDL to create `silver.klines_raw` table (ReplacingMergeTree)
3. Reads Hive-partitioned Parquet directly from MinIO via PyArrow dataset
4. Projects and casts columns to match the defined schema
5. Inserts into ClickHouse via `insert_arrow()` (zero-copy columnar path)
6. Returns the total row count inserted

**Key details:**

- `ReplacingMergeTree(_loaded_at)` ensures idempotent re-loads (dedup on background merge)
- Partitioned by `(symbol, toYYYYMM(open_time))`
- Ordered by `(symbol, interval, open_time)`

---

## Justfile Shortcuts

The `justfile` provides shortcut recipes for all commands:

| Recipe | Command | Description |
|--------|---------|-------------|
| `just produce` | `uv run produce` | Start live producer |
| `just write-lake` | `uv run write-lake` | Start lake writer |
| `just backfill` | `uv run backfill` | Backfill historical data |
| `just silver` | `uv run silver` | Run silver transformation |
| `just load-olap` | `uv run load-olap` | Load silver → ClickHouse |
| `just dbt-deps` | `cd dbt && dbt deps` | Install dbt packages |
| `just dbt-run` | `cd dbt && dbt run` | Run all dbt models |
| `just dbt-test` | `cd dbt && dbt test` | Run all dbt tests |
| `just pc` | `uv run pre-commit run` | Run pre-commit hooks |
| `just check` | `uv run ruff check .` | Lint code |
| `just format` | `uv run ruff format .` | Format code |
| `just mypy` | `uv run mypy .` | Type check |
| `just docs` | `uv run mkdocs serve` | Serve docs locally |

---

## Entry Point Registration

All commands are registered in `pyproject.toml`:

```toml
[project.scripts]
produce = "ingestion.run_producer:cli"
write-lake = "ingestion.run_lake_writer:cli"
backfill = "batch.run_backfill:cli"
silver = "batch.run_silver:cli"
load-olap = "olap.run_loader:cli"
```

Each `run_*.py` module contains a `cli()` function that:

1. Loads configuration from environment/`.env`
2. Sets up structured logging
3. Runs the async event loop (or PySpark job / ClickHouse loader)
4. Handles graceful shutdown via signal handlers
