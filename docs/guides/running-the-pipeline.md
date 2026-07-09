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

## Monitoring

### Kafka UI

Open [http://localhost:8080](http://localhost:8080) to:

- View topic message counts
- Inspect individual messages
- Monitor consumer group lag
- Browse partition distribution

### MinIO Console

Open [http://localhost:9001](http://localhost:9001) (login: `minioadmin` / `minioadmin`):

- Browse bronze and silver buckets
- View Parquet file metadata
- Download individual files
- Monitor storage usage

### Logs

All components output structured logs with timestamps:

```bash
# Production logs are JSON-formatted for aggregation
# Development logs are human-readable console output
```

Set `*_LOG_LEVEL=DEBUG` for verbose output.

## Common Operations

### Re-run Silver Transformation

```bash
# Re-run after new bronze data arrives
uv run silver
```

Silver overwrites are atomic — new data replaces old data for each symbol.

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
