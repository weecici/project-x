# Environment Variables

Complete reference of all environment variables used by the platform.

## Storage (MinIO / S3)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | `str` | `minioadmin` | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | `str` | `minioadmin` | S3 secret key |
| `AWS_ENDPOINT_URL` | `str` | `http://localhost:9000` | S3/MinIO endpoint URL |
| `AWS_DEFAULT_REGION` | `str` | `us-east-1` | AWS region (required by boto3/pyarrow) |
| `AWS_S3_BUCKET_BRONZE` | `str` | `bronze` | Bronze bucket name |
| `AWS_S3_BUCKET_SILVER` | `str` | `silver` | Silver bucket name |

## Ingestion (Live Pipeline)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `INGESTION_SYMBOLS` | `list[str]` | `["BTCUSDT", "ETHUSDT"]` | Trading pairs to subscribe to |
| `INGESTION_INTERVALS` | `list[str]` | `["1m"]` | Kline intervals to subscribe to |
| `INGESTION_WS_URL` | `str` | `wss://stream.binance.com:9443/ws/` | Binance WebSocket endpoint |
| `INGESTION_KAFKA_BROKER` | `str` | `localhost:9094` | Kafka broker address |
| `INGESTION_FLUSH_INTERVAL_SECONDS` | `float` | `30.0` | Seconds between lake flushes |
| `INGESTION_FLUSH_THRESHOLD` | `int` | `1000` | Max messages before forced flush |
| `INGESTION_KAFKA_POLL_TIMEOUT` | `float` | `0.5` | Kafka consumer poll timeout (seconds) |
| `INGESTION_LOG_LEVEL` | `str` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |

## Batch — Backfill

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `BACKFILL_SYMBOLS` | `list[str]` | `["BTCUSDT"]` | Symbols to backfill |
| `BACKFILL_INTERVALS` | `list[str]` | `["1m"]` | Kline intervals to backfill |
| `BACKFILL_START_TIME` | `datetime` | 30 days ago | Backfill start (ISO format) |
| `BACKFILL_END_TIME` | `datetime \| None` | `None` (latest) | Backfill end, None = now |
| `BACKFILL_LIMIT` | `int` | `1000` | Max candles per API request |
| `BACKFILL_MAX_RETRIES` | `int` | `3` | Retry count per chunk |
| `BACKFILL_CHUNK_DAYS` | `int` | `7` | Days per chunk |
| `BACKFILL_LOG_LEVEL` | `str` | `INFO` | Log level |

## Batch — Silver Transformer

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SILVER_INPUT_PATH` | `str` | `bronze/klines` | Input bronze path |
| `SILVER_OUTPUT_PATH` | `str` | `silver/klines` | Output silver path |
| `SILVER_LOG_LEVEL` | `str` | `INFO` | Log level |

## Docker Compose

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COMPOSE_PROJECT_NAME` | `str` | `platform` | Docker Compose project name |

## Loading Order

Configuration is loaded in this priority order (highest to lowest):

1. **Environment variables** (set in shell or CI)
2. **`.env` file** (loaded by `dotenv_values()`)
3. **Hardcoded defaults** (in `Settings` inner class)

```python
# Example: overriding via env var
BACKFILL_SYMBOLS='["BTCUSDT", "ETHUSDT"]' uv run backfill

# Example: overriding via .env file
echo 'BACKFILL_SYMBOLS=["BTCUSDT"]' >> .env
```

## `.env` File Format

The `.env` file supports standard shell syntax:

```bash
# Comments start with #
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT_URL=http://localhost:9000

# Lists use JSON syntax
INGESTION_SYMBOLS='["BTCUSDT", "ETHUSDT", "SOLUSDT"]'

# Datetime uses ISO format
BACKFILL_START_TIME=2026-01-01T00:00:00Z

# Boolean-like strings
INGESTION_LOG_LEVEL=DEBUG
```

!!! warning "Export prefix supported"
    The config loader strips `export ` prefixes, so `.env` files written by `export` statements also work.
