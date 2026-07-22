# Running the Pipeline

Step-by-step guide to operating each component of the platform.

## Start Infrastructure

Before running any pipeline components, start the required services:

```bash
docker compose up -d
```

Verify all services are healthy:

```bash
docker compose ps
```

Expected services:

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9094 | Message broker |
| Kafka UI | 8080 | Web UI |
| MinIO | 9000, 9001 | S3 storage + console |
| ClickHouse | 8123, 9009 | OLAP database |
| mc-init | — | Bucket creation (one-shot) |

Stop services when done:

```bash
docker compose stop       # Keeps containers for faster restart
docker compose down       # Removes containers and networks
docker compose down -v    # Removes everything including data
```

## Live Pipeline

The live pipeline streams real-time data from Binance into the data lake.

### Start the Producer

Connects to Binance WebSocket, publishes trades and klines to Kafka:

```bash
uv run produce
```

The producer will:

1. Connect to Binance WebSocket streams
2. Subscribe to configured symbols and intervals
3. Publish messages to `raw.trades` and `raw.klines` topics
4. Handle reconnection on disconnects
5. Gracefully shut down on `Ctrl+C`

### Start the Lake Writer

Consumes from Kafka, writes Parquet files to MinIO:

```bash
uv run write-lake
```

The lake writer will:

1. Start consuming from all partitions
2. Buffer messages in memory
3. Flush to MinIO bronze when thresholds are met (30s or 1000 messages)
4. Flush remaining messages on `Ctrl+C`

### Running Both Together

Run them in separate terminals:

```bash
# Terminal 1
uv run produce

# Terminal 2
uv run write-lake
```

Or use a process manager:

```bash
uv run produce &
uv run write-lake &
wait
```

## Batch Pipeline

The batch pipeline handles historical data and transformations.

### Backfill Historical Data

Fetch historical kline data from Binance REST API:

```bash
uv run backfill
```

Configure the backfill via environment variables:

```bash
BACKFILL_SYMBOLS='["BTCUSDT", "ETHUSDT"]' \
BACKFILL_START_TIME=2026-01-01T00:00:00Z \
BACKFILL_CHUNK_DAYS=14 \
uv run backfill
```

The backfill will:

1. Split the date range into chunks (7 days each by default)
2. Fetch 1000 candles per API request
3. Apply rate limiting (1200 requests/minute)
4. Write each chunk as a Parquet file to bronze
5. Report results (`BackfillResult`)

### Run Silver Transformation

Transform bronze data into clean, deduplicated silver:

```bash
uv run silver
```

The transformer will:

1. Read all bronze Parquet files via PySpark
2. Deduplicate by `symbol + open_time`
3. Cast types (strings → decimals)
4. Repartition for write performance
5. Overwrite silver layer atomically
6. Report statistics (`SilverResult`)

## OLAP Pipeline

Load silver data into ClickHouse and build gold-layer analytics tables.

### Load Silver into ClickHouse

```bash
uv run load-olap
```

The loader will:

1. Create `silver` and `gold` databases if they don't exist
2. Create `silver.klines_raw` table (ReplacingMergeTree)
3. Read Hive-partitioned Parquet directly from MinIO
4. Insert into ClickHouse via zero-copy Arrow path
5. Report total rows inserted

### Run dbt Models

Install dbt dependencies, then run all models:

```bash
# Install dbt packages (dbt_utils)
uv run dbt deps

# Run all models (staging + marts)
uv run dbt run

# Run tests
uv run dbt test
```

Or via just shortcuts:

```bash
just dbt-deps
just dbt-run
just dbt-test
```

The dbt run will:

1. Create `silver.stg_crypto__klines` view (typed, deduplicated)
2. Create `gold.fct_daily_klines` table (daily OHLCV)
3. Create `gold.fct_hourly_klines` table (hourly OHLCV)
4. Create `gold.fct_kline_returns` table (log returns)

## Streaming (Phase 4)

Start the streaming jobs **after** the live producer is running:

```bash
# Terminal 3: OHLCV streaming
uv run stream-ohlcv

# Terminal 4: VWAP streaming
uv run stream-vwap
```

Or via just shortcuts:

```bash
just stream-ohlcv
just stream-vwap
```

The streaming jobs:

1. **OHLCV**: Reads `raw.klines` from Kafka, filters closed bars, casts to Silver types, and dual-sinks to Delta Lake + `agg.klines` Kafka topic
2. **VWAP**: Reads `raw.trades` from Kafka, applies event-time watermarking, deduplicates, and computes VWAP, OFI, volatility, and trade count via tumbling windows — dual-sinks to Delta Lake + `agg.vwap` Kafka topic

**Key details:**

- Streaming jobs run continuously — kill to stop
- Checkpoint-based exactly-once semantics (S3 checkpoints)
- Only the VWAP job uses event-time watermarking (10s default); OHLCV is a filter-and-cast pipeline
- Delta Lake provides ACID transactions on MinIO

## Monitoring

### Kafka UI

Open [http://localhost:8080](http://localhost:8080) to:

- View topic message counts
- Inspect individual messages
- Monitor consumer group lag
- Browse partition distribution

### MinIO Console

Open [http://localhost:9001](http://localhost:9001) (login: `minioadmin` / `minioadmin`):

- Browse bronze, silver, and gold buckets
- View Parquet file metadata
- Download individual files
- Monitor storage usage

### ClickHouse

Query ClickHouse directly via the HTTP interface:

```bash
# Count rows in silver
curl 'http://localhost:8123/?query=SELECT+count()+FROM+silver.klines_raw+FINAL'

# Query gold marts
curl 'http://localhost:8123/?query=SELECT+*+FROM+gold.fct_daily_klines+LIMIT+10'

# List databases
curl 'http://localhost:8123/?query=SHOW+DATABASES'
```

### Logs

All components output structured logs with timestamps:

```bash
# Production logs are JSON-formatted for aggregation
# Development logs are human-readable console output
```

Set `*_LOG_LEVEL=DEBUG` for verbose output.

## Common Operations

### Full Pipeline (End-to-End)

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Live pipeline (Terminal 1 + 2)
uv run produce &
uv run write-lake &

# 3. Backfill historical data
uv run backfill

# 4. Transform to silver
uv run silver

# 5. Load into ClickHouse
uv run load-olap

# 6. Build gold tables
uv run dbt deps
uv run dbt run
uv run dbt test

# 7. Start streaming (Terminal 3 + 4)
uv run stream-ohlcv &
uv run stream-vwap &
```

### Re-run Silver Transformation

```bash
# Re-run after new bronze data arrives
uv run silver
```

Silver overwrites are atomic — new data replaces old data for each symbol.

### Re-load OLAP

```bash
# Re-run after new silver data arrives
uv run load-olap
```

ClickHouse ReplacingMergeTree ensures idempotent loads (dedup on background merge).

### Re-run dbt Models

```bash
# Re-run all models
uv run dbt run
```

dbt models are idempotent — re-running produces the same result.

### Re-backfill a Date Range

```bash
BACKFILL_START_TIME=2026-07-01T00:00:00Z \
BACKFILL_END_TIME=2026-07-09T00:00:00Z \
uv run backfill
```

UUID-based filenames prevent duplicates — re-running produces the same data.

### Stop Everything

```bash
# Stop pipeline components
kill $(jobs -p)

# Stop infrastructure
docker compose stop
```

## Troubleshooting

### Kafka connection refused

Ensure Kafka is running and healthy:

```bash
docker compose ps kafka
docker compose logs kafka --tail 20
```

Wait for the "started" message in logs before starting producers.

### MinIO connection refused

Ensure MinIO is running:

```bash
docker compose ps minio
docker compose logs minio --tail 20
```

Verify credentials in `.env` match `docker-compose.yaml`.

### ClickHouse connection refused

Ensure ClickHouse is running:

```bash
docker compose ps clickhouse
docker compose logs clickhouse --tail 20
```

Test connectivity:

```bash
curl 'http://localhost:8123/?query=SELECT+1'
```

### ClickHouse OOM

If ClickHouse queries are killed due to memory:

- Check `max_memory_usage` in `infra/clickhouse/users.d/custom-users.xml` (default: 256 MB)
- Check `mem_limit` in `docker-compose.yaml` (default: 2048m)
- Reduce query complexity or increase limits

### PySpark won't start

Ensure Java is available:

```bash
java -version  # Needs JDK 11+
```

PySpark runs in local mode with `local[*]` (all cores).

### Rate limiting (429 errors)

The backfiller uses built-in rate limiting. If you see 429 errors:

- Reduce `BACKFILL_CHUNK_DAYS` to smaller chunks
- The retry mechanism handles transient 429s automatically

### dbt fails with adapter error

Ensure `DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true` is set (already configured in justfile):

```bash
just dbt-run
```

Or set it manually:

```bash
DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt run
```
