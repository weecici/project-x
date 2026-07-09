# Configuration

All configuration is managed through environment variables and `.env` files, validated by Pydantic v2 Settings.

## How Configuration Works

Both `IngestionConfig` and `BatchConfig` extend `pydantic_settings.BaseSettings`:

1. **Environment variables** are checked first (highest priority)
2. **`.env` file** is loaded as fallback
3. **Hardcoded defaults** in the `Settings` inner class provide final fallback

This means you can override any setting via env var without changing code.

## Ingestion Configuration

These control the live WebSocket → Kafka → lake pipeline.

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `INGESTION_SYMBOLS` | `list[str]` | `["BTCUSDT", "ETHUSDT"]` | Trading pairs to subscribe to |
| `INGESTION_INTERVALS` | `list[str]` | `["1m"]` | Kline intervals to subscribe to |
| `INGESTION_WS_URL` | `str` | `wss://stream.binance.com:9443/ws/` | Binance WebSocket endpoint |
| `INGESTION_KAFKA_BROKER` | `str` | `localhost:9094` | Kafka broker address |
| `INGESTION_FLUSH_INTERVAL_SECONDS` | `float` | `30.0` | Seconds between lake flushes |
| `INGESTION_FLUSH_THRESHOLD` | `int` | `1000` | Max messages before forced flush |
| `INGESTION_KAFKA_POLL_TIMEOUT` | `float` | `0.5` | Kafka consumer poll timeout (seconds) |
| `INGESTION_LOG_LEVEL` | `str` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |

**Kafka Topics:**

| Topic | Data |
|-------|------|
| `raw.trades` | Live trade events |
| `raw.klines` | Live kline/candlestick events |

**Example `.env`:**

```bash
INGESTION_SYMBOLS='["BTCUSDT", "ETHUSDT", "SOLUSDT"]'
INGESTION_INTERVALS='["1m", "5m", "1h"]'
INGESTION_FLUSH_INTERVAL_SECONDS=15
INGESTION_LOG_LEVEL=DEBUG
```

## Batch Configuration

These control the REST backfill and silver transformation.

### Backfill Settings

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `BACKFILL_SYMBOLS` | `list[str]` | `["BTCUSDT"]` | Symbols to backfill |
| `BACKFILL_INTERVALS` | `list[str]` | `["1m"]` | Kline intervals to backfill |
| `BACKFILL_START_TIME` | `datetime` | 30 days ago | Backfill start (ISO format) |
| `BACKFILL_END_TIME` | `datetime | None` | `None` (latest) | Backfill end, None = now |
| `BACKFILL_LIMIT` | `int` | `1000` | Max candles per API request |
| `BACKFILL_MAX_RETRIES` | `int` | `3` | Retry count per chunk |
| `BACKFILL_CHUNK_DAYS` | `int` | `7` | Days per chunk |
| `BACKFILL_LOG_LEVEL` | `str` | `INFO` | Log level |

### Silver Transformer Settings

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `SILVER_INPUT_PATH` | `str` | `bronze/klines` | Input bronze path |
| `SILVER_OUTPUT_PATH` | `str` | `silver/klines` | Output silver path |
| `SILVER_LOG_LEVEL` | `str` | `INFO` | Log level |

**Example `.env`:**

```bash
BACKFILL_SYMBOLS='["BTCUSDT", "ETHUSDT"]'
BACKFILL_START_TIME=2026-01-01T00:00:00Z
BACKFILL_CHUNK_DAYS=14
SILVER_LOG_LEVEL=DEBUG
```

## AWS/MinIO Storage

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | `str` | `minioadmin` | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | `str` | `minioadmin` | S3 secret key |
| `AWS_ENDPOINT_URL` | `str` | `http://localhost:9000` | S3/MinIO endpoint |
| `AWS_DEFAULT_REGION` | `str` | `us-east-1` | AWS region (required by boto3/pyarrow) |
| `AWS_S3_BUCKET_BRONZE` | `str` | `bronze` | Bronze bucket name |
| `AWS_S3_BUCKET_SILVER` | `str` | `silver` | Silver bucket name |

## Docker Compose Ports

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9094 | Broker |
| Kafka UI | 8080 | Web UI |
| MinIO API | 9000 | S3 API |
| MinIO Console | 9001 | Web console |
