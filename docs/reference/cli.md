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
| **Config** | `KAFKA_*`, `BINANCE_WS_BASE_URL`, `SYMBOLS`, `KLINE_INTERVALS`, `MINIO_*` env vars |

**What it does:**

1. Connects to Binance WebSocket streams for configured symbols/intervals
2. Parses each message into a typed Pydantic model (`TradeEvent` or `KlineEvent`)
3. Partitions messages by symbol and publishes to the correct Kafka partition
4. Handles reconnection automatically on disconnects
5. Logs message delivery at DEBUG level

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
| **Config** | `KAFKA_*`, `MINIO_*`, `LAKE_FLUSH_ROWS`, `LAKE_FLUSH_SECONDS` env vars |

**What it does:**

1. Starts consuming from all partitions of `raw.trades` and `raw.klines`
2. Buffers messages in memory
3. Flushes to MinIO when either threshold is met:
    - 30 seconds since last flush (`LAKE_FLUSH_SECONDS`)
    - 1,000 messages buffered (`LAKE_FLUSH_ROWS`)
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
| **Config** | `BINANCE_*`, `SYMBOLS`, `KLINE_INTERVALS`, `BACKFILL_*`, `MINIO_*`, `SPARK_*` env vars |

**What it does:**

1. Iterates through the date range, fetching up to 1000 bars per API request
2. Applies rate limiting (2 tokens per request, 1200 tokens/minute)
3. Retries failed requests up to 5 times with exponential backoff
4. Writes each chunk as a Parquet file to bronze
5. Reports the total row count inserted

**Example:**

```bash
SYMBOLS='["BTCUSDT"]' \
BACKFILL_START_DATE=2024-01-01 \
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
| **Config** | `MINIO_*`, `SPARK_*` env vars |

**What it does:**

1. Reads all bronze kline Parquet files via PySpark
2. Deduplicates by `symbol + interval + open_time` (keeps latest)
3. Casts types (strings → decimals, longs → timestamps)
4. Repartitions by `symbol/interval/year/month` (Hive-style partitioning)
5. Overwrites silver layer atomically
6. Logs input/output/duplicate counts

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

## `stream-ohlcv`

Start the OHLCV Structured Streaming job.

```bash
uv run stream-ohlcv
```

| Behavior | Detail |
|----------|--------|
| **Input** | Kafka `raw.klines` topic |
| **Output** | Delta Lake `s3a://silver/klines_stream/` + Kafka `agg.klines` |
| **Config** | `KAFKA_*`, `MINIO_*`, `SPARK_*`, `STREAM_*` env vars |

**What it does:**

1. Reads kline JSON from Kafka, parses nested kline schema
2. Casts OHLCV fields to `DecimalType(18, 8)`
3. Filters only closed bars (`is_closed == True`)
4. **Dual-sink**: Writes to Delta Lake + produces to `agg.klines` Kafka topic
5. Checkpoint-based exactly-once semantics (S3 checkpoints)

**When to run:**

- Run after starting the live producer (`uv run produce`)
- Streams continuously — kill to stop

---

## `stream-vwap`

Start the VWAP Structured Streaming job.

```bash
uv run stream-vwap
```

| Behavior | Detail |
|----------|--------|
| **Input** | Kafka `raw.trades` topic |
| **Output** | Delta Lake `s3a://silver/vwap_stream/` + Kafka `agg.vwap` |
| **Config** | `KAFKA_*`, `MINIO_*`, `SPARK_*`, `STREAM_*` env vars |

**What it does:**

1. Reads trade JSON from Kafka, casts price/quantity to `DoubleType`
2. Parses `trade_time` (epoch millis) into a `TimestampType` column
3. Applies event-time watermark on `trade_time_ts`
4. Deduplicates within watermark using `trade_id` (stateful)
5. Tumbling window aggregation (1 minute default) computing:
    - **VWAP**: `sum(price * quantity) / sum(quantity)`
    - **Order Flow Imbalance**: `sum(qty for taker buys) - sum(qty for maker buys)`
    - **Price Volatility**: `coalesce(stddev_samp(price), 0.0)`
    - **Trade Count**: `count(*)`
6. **Dual-sink**: Writes to Delta Lake + produces to `agg.vwap` Kafka topic

**When to run:**

- Run after starting the live producer (`uv run produce`)
- Streams continuously — kill to stop

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
| `just stream-ohlcv` | `uv run stream-ohlcv` | Start OHLCV streaming job |
| `just stream-vwap` | `uv run stream-vwap` | Start VWAP streaming job |
| `just dbt-deps` | `cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt deps` | Install dbt packages |
| `just dbt-run` | `cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt run` | Run all dbt models |
| `just dbt-test` | `cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt test` | Run all dbt tests |
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
stream-ohlcv = "streaming.run_ohlcv:cli"
stream-vwap = "streaming.run_vwap:cli"
```

Each `run_*.py` module contains a `cli()` function that:

1. Loads configuration from environment/`.env`
2. Sets up structured logging
3. Runs the async event loop (or PySpark job / ClickHouse loader)
4. Handles graceful shutdown via signal handlers
