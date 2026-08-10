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

## `export-bi`

Export Cube.js semantic layer data to CSV and sync to Google Sheets.

```bash
uv run export-bi
```

| Behavior | Detail |
|----------|--------|
| **Input** | Cube.js REST API (port 4000) |
| **Output** | Local CSV files + Google Sheets sync |
| **Config** | `CUBE_API_URL`, `CUBE_API_SECRET`, `EXPORT_OUTPUT_DIR`, `GOOGLE_SERVICE_ACCOUNT_FILE`, `GOOGLE_SHEET_NAME` env vars |

**What it does:**

1. Connects to Cube.js REST API
2. Fetches data for each configured view (`ohlcv_daily`, `ohlcv_hourly`, `price_analytics`)
3. Saves local CSV files to the output directory
4. Optionally syncs to Google Sheets via `gspread` (if service account configured)

---

## `export-lineage`

Export OpenMetadata-compatible lineage manifest from the platform.

```bash
uv run export-lineage
```

| Behavior | Detail |
|----------|--------|
| **Input** | Runtime configs, dbt manifest, Airflow DAGs, Cube YAML, BI exporter config |
| **Output** | `lineage_manifest.json` in the exports directory |
| **Config** | `LINEAGE_OUTPUT_DIR`, `OPENLINEAGE_NAMESPACE`, `OPENMETADATA_URL` env vars |

**What it does:**

1. Extracts lineage from 5 sources: runtime configs, dbt manifest, Airflow DAGs, Cube schemas, BI exporter config
2. Deduplicates nodes and edges across all sources
3. Generates OpenLineage v1.0 RunEvents and OpenMetadata AddLineageRequest payloads
4. Writes the manifest as JSON to `LINEAGE_OUTPUT_DIR/lineage_manifest.json`

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

## `feature-eng`

Run the ML feature engineering pipeline.

```bash
uv run feature-eng
```

| Behavior | Detail |
|----------|--------|
| **Input** | MinIO silver klines (Parquet) + CSV seed data |
| **Output** | MinIO gold feature matrix + MLflow metrics |
| **Config** | `SYMBOLS`, `KLINE_INTERVALS`, `MINIO_*`, `SPARK_*`, `MLFLOW_*` env vars |

**What it does:**

1. Connects to MinIO and reads silver-layer kline Parquet data
2. Computes technical indicators via Numba JIT (EMA, RSI, MACD) with up to 97x speedup
3. Adds rolling statistics (SMA, volatility), lagged returns, and target labels
4. Joins BTC dominance and Fear & Greed features where available
5. Applies standard scaling to feature matrix
6. Writes gold Parquet to MinIO and logs metrics to MLflow

---

## `train-model`

Train the CryptoLSTM price direction prediction model.

```bash
uv run train-model
```

| Behavior | Detail |
|----------|--------|
| **Input** | MinIO gold feature matrix (Parquet) |
| **Output** | Registered MLflow model + benchmark metrics |
| **Config** | `SYMBOLS`, `MINIO_*`, `MLFLOW_*`, `TRAINING_*` env vars |

**What it does:**

1. Loads gold feature Parquet from MinIO into a PyTorch Dataset
2. Builds sequence windows (N, 60, 11) with temporal train/val split
3. Initializes CryptoLSTM (stacked LSTM + Dropout + Linear head)
4. Trains with Adam optimizer, BCE loss, mixed precision (AMP), early stopping
5. Logs metrics, parameters, and model artifacts to MLflow
6. Registers as Champion/Challenger in MLflow Model Registry
7. Exports ONNX model and saves benchmark results

---

## `optimize-model`

Optimize a trained model for production deployment.

```bash
uv run optimize-model
```

| Behavior | Detail |
|----------|--------|
| **Input** | MLflow registered model (champion or challenger) |
| **Output** | Optimized ONNX model + benchmark report |
| **Config** | `MLFLOW_*`, `OPTIMIZATION_*`, `ONNX_*` env vars |

**What it does:**

1. Fetches the latest model from MLflow Model Registry
2. Runs torch.compile optimization (reduce-overhead mode)
3. Exports to ONNX format (opset 17) for cross-platform deployment
4. Applies global L1 unstructured pruning (30% sparsity) + fine-tuning
5. Applies dynamic INT8 quantization (5.07MB -> 1.29MB, ~74% size reduction)
6. Benchmarks all 4 variants (Baseline, JIT, Pruned, Quantized) for latency and accuracy
7. Writes optimized models + benchmark CSV/JSON to MinIO artifacts

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
| `just export-lineage` | `uv run export-lineage` | Export lineage manifest |
| `just feature-eng` | `uv run feature-eng` | Run ML feature engineering |
| `just train` | `uv run train-model` | Train CryptoLSTM model |
| `just optimize` | `uv run optimize-model` | Run model optimization |
| `just dbt-deps` | `cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt deps` | Install dbt packages |
| `just dbt-run` | `cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt run` | Run all dbt models |
| `just dbt-test` | `cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt test` | Run all dbt tests |
| `just pc` | `uv run pre-commit run --all-files` | Run pre-commit hooks |
| `just check` | `uv run ruff check . --fix --exit-non-zero-on-fix` | Lint code |
| `just format` | `uv run ruff format .` | Format code |
| `just mypy` | `uv run mypy .` | Type check |
| `just docs` | `uv run mkdocs serve` | Serve docs locally |
| `just up` | `docker compose up -d` | Start all infrastructure |
| `just up obs` | `docker compose --profile obs up -d` | Start observability stack |
| `just up ml` | `docker compose --profile ml up -d mlflow` | Start MLflow server |
| `just down` | `docker compose down` | Stop all infrastructure |
| `just down obs` | `docker compose --profile obs down` | Stop observability stack |
| `just down ml` | `docker compose --profile ml down` | Stop MLflow server |
| `just reload-prom` | `curl -X POST http://localhost:9090/-/reload` | Hot-reload Prometheus config |
| `just airflow-init` | Airflow DB migrate + create admin user | Initialize Airflow |
| `just airflow-up` | `uv run airflow standalone` | Start Airflow webserver + scheduler |

---

## Entry Point Registration

All commands are registered in `pyproject.toml`:

```toml
[project.scripts]
produce = "ingestion.run_producer:cli"
write-lake = "ingestion.run_lake_writer:cli"
backfill = "batch.run_backfill:cli"
silver = "batch.run_silver:cli"
load-olap = "olap.loader:cli"
stream-ohlcv = "streaming.run_ohlcv:cli"
stream-vwap = "streaming.run_vwap:cli"
export-bi = "olap.exporter:cli"
export-lineage = "orchestration.governance.run_lineage:cli"
feature-eng = "ml.features.run_feature_eng:main"
train-model = "ml.training.run_train:main"
optimize-model = "ml.optimization.run_optimize:main"
```

Each `run_*.py` module contains a `cli()` function that:

1. Loads configuration from environment/`.env`
2. Sets up structured logging
3. Runs the async event loop (or PySpark job / ClickHouse loader)
4. Handles graceful shutdown via signal handlers
