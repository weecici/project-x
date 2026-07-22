# Environment Variables

Complete reference of all environment variables used by the platform.

## Storage (MinIO / S3)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `MINIO_ENDPOINT` | `str` | `http://localhost:9000` | MinIO endpoint URL |
| `MINIO_ACCESS_KEY` | `str` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `str` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET_BRONZE` | `str` | `bronze` | Bronze bucket name |
| `MINIO_BUCKET_SILVER` | `str` | `silver` | Silver bucket name |
| `AWS_DEFAULT_REGION` | `str` | `us-east-1` | AWS region (required by boto3/pyarrow) |

## Ingestion (Live Pipeline)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `str` | `localhost:9094` | Comma-separated Kafka bootstrap servers |
| `KAFKA_TOPIC_TRADES` | `str` | `raw.trades` | Topic containing raw trade events |
| `KAFKA_TOPIC_KLINES` | `str` | `raw.klines` | Topic containing raw kline events |
| `KAFKA_DLQ_TRADES` | `str` | `raw.trades.dlq` | Dead-letter topic for trades that fail validation |
| `KAFKA_DLQ_KLINES` | `str` | `raw.klines.dlq` | Dead-letter topic for klines that fail validation |
| `MINIO_ENDPOINT` | `str` | `http://localhost:9000` | MinIO S3-compatible endpoint |
| `MINIO_ACCESS_KEY` | `str` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `str` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET_BRONZE` | `str` | `bronze` | Bronze bucket name |
| `BINANCE_WS_BASE_URL` | `str` | `wss://stream.binance.com:9443` | Binance combined-stream WebSocket base URL |
| `SYMBOLS` | `list[str]` | `["BTCUSDT", "ETHUSDT"]` | Trading-pair symbols to subscribe to |
| `KLINE_INTERVALS` | `list[str]` | `["1m"]` | Kline intervals to subscribe to |
| `LAKE_FLUSH_ROWS` | `int` | `1000` | Flush to MinIO after this many buffered rows |
| `LAKE_FLUSH_SECONDS` | `int` | `30` | Flush to MinIO after this many seconds |

## Batch — Backfill

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `BINANCE_REST_BASE_URL` | `str` | `https://api.binance.com` | Base URL for Binance Spot REST API |
| `BINANCE_API_KEY` | `str` | `""` | Optional Binance API key (raises rate-limit from 1200 to 6000/min) |
| `SYMBOLS` | `list[str]` | `["BTCUSDT", "ETHUSDT"]` | Trading-pair symbols to backfill |
| `KLINE_INTERVALS` | `list[str]` | `["1m", "1h", "1d"]` | Kline intervals to backfill |
| `BACKFILL_START_DATE` | `str` | `2024-01-01` | ISO-8601 date (YYYY-MM-DD); start of historical fetch window |
| `BACKFILL_END_DATE` | `str` | `""` | ISO-8601 date (YYYY-MM-DD); empty string defaults to today (UTC) |
| `MINIO_ENDPOINT` | `str` | `http://localhost:9000` | MinIO S3-compatible endpoint |
| `MINIO_ACCESS_KEY` | `str` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `str` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET_BRONZE` | `str` | `bronze` | Bronze bucket name |
| `MINIO_BUCKET_SILVER` | `str` | `silver` | Silver bucket name |
| `SPARK_DRIVER_MEMORY` | `str` | `1g` | JVM heap for the Spark driver process |
| `SPARK_EXECUTOR_MEMORY` | `str` | `1g` | JVM heap for the Spark executor process |

## ClickHouse (OLAP)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CLICKHOUSE_HOST` | `str` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `int` | `8123` | HTTP interface port |
| `CLICKHOUSE_DB` | `str` | `silver` | Target database for OLAP loader |
| `CLICKHOUSE_USER` | `str` | `default` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | `str` | `""` | ClickHouse password |
| `CLICKHOUSE_TABLE_KLINES` | `str` | `klines_raw` | Target ClickHouse table for kline data |
| `MINIO_ENDPOINT` | `str` | `http://localhost:9000` | MinIO S3-compatible endpoint |
| `MINIO_ACCESS_KEY` | `str` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `str` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET_SILVER` | `str` | `silver` | Silver bucket name |
| `SILVER_KLINES_PREFIX` | `str` | `klines/` | S3 prefix under the silver bucket for kline Parquet files |

!!! note "Database naming"
    The OLAP loader defaults to `silver` database. The docker-compose sets `CLICKHOUSE_DB=gold` for the ClickHouse container's default database. dbt targets `gold` via `profiles.yml`. These are independent: the loader writes to `silver`, dbt reads from `silver` and writes to `gold`.

## Streaming

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `str` | `localhost:9094` | Comma-separated list of Kafka bootstrap servers |
| `KAFKA_TOPIC_TRADES` | `str` | `raw.trades` | Topic containing raw tick-by-tick trade executions |
| `KAFKA_TOPIC_KLINES` | `str` | `raw.klines` | Topic containing raw kline update events |
| `KAFKA_TOPIC_AGG_KLINES` | `str` | `agg.klines` | Downstream topic for finalized streaming klines |
| `KAFKA_TOPIC_AGG_VWAP` | `str` | `agg.vwap` | Downstream topic for finalized streaming VWAP/microstructure metrics |
| `KAFKA_STARTING_OFFSETS` | `str` | `latest` | Starting offsets for Kafka streams (e.g. earliest, latest) |
| `MINIO_ENDPOINT` | `str` | `http://localhost:9000` | MinIO S3-compatible service URL |
| `MINIO_ACCESS_KEY` | `str` | `minioadmin` | MinIO root access key |
| `MINIO_SECRET_KEY` | `str` | `minioadmin` | MinIO root secret key |
| `MINIO_BUCKET_SILVER` | `str` | `silver` | Bucket where silver Delta tables are written |
| `SPARK_DRIVER_MEMORY` | `str` | `1g` | JVM heap for the local Spark driver process |
| `SPARK_EXECUTOR_MEMORY` | `str` | `1g` | JVM heap for the local Spark executor process |
| `STREAM_WATERMARK_DELAY_SECONDS` | `int` | `10` | Allowed threshold (seconds) for late-arriving trade ticks |
| `STREAM_WINDOW_DURATION_MINUTES` | `int` | `1` | Duration (minutes) for tumbling aggregation windows |

## dbt

dbt uses the same `CLICKHOUSE_*` environment variables as the OLAP loader, referenced in `dbt/profiles.yml`:

| Variable | Used By | Default |
|----------|---------|---------|
| `CLICKHOUSE_HOST` | `profiles.yml` | `localhost` |
| `CLICKHOUSE_PORT` | `profiles.yml` | `8123` |
| `CLICKHOUSE_USER` | `profiles.yml` | `default` |
| `CLICKHOUSE_PASSWORD` | `profiles.yml` | `""` |

Additionally, `DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true` must be set when running dbt commands (already configured in the justfile).

## Docker Compose

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COMPOSE_PROJECT_NAME` | `str` | `platform` | Docker Compose project name |

## Loading Order

Configuration is loaded in this priority order (highest to lowest):

1. **Environment variables** (set in shell or CI)
2. **`.env` file** (loaded by `dotenv_values()`)
3. **Hardcoded defaults** (in each config class)

```python
# Example: overriding via env var
SYMBOLS='["BTCUSDT", "ETHUSDT"]' uv run backfill

# Example: overriding via .env file
echo 'SYMBOLS=["BTCUSDT"]' >> .env
```

## `.env` File Format

The `.env` file supports standard shell syntax:

```bash
# Comments start with #
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin

# Lists use JSON syntax
SYMBOLS='["BTCUSDT", "ETHUSDT", "SOLUSDT"]'

# Date uses YYYY-MM-DD format
BACKFILL_START_DATE=2024-01-01

# ClickHouse connection
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DB=silver
```

!!! warning "Export prefix supported"
    The config loader strips `export ` prefixes, so `.env` files written by `export` statements also work.
