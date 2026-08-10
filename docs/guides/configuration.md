# Configuration

All configuration is managed through environment variables and `.env` files, validated by Pydantic v2 Settings.

## How Configuration Works

All config classes (`IngestionConfig`, `BatchConfig`, `OlapConfig`, `StreamingConfig`) extend `pydantic_settings.BaseSettings`:

1. **Environment variables** are checked first (highest priority)
2. **`.env` file** is loaded as fallback
3. **Hardcoded defaults** in each config class provide final fallback

This means you can override any setting via env var without changing code.

## Ingestion Configuration

These control the live WebSocket → Kafka → lake pipeline.

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
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

**Kafka Topics:**

| Topic | Data |
|-------|------|
| `raw.trades` | Live trade events |
| `raw.klines` | Live kline/candlestick events |
| `raw.trades.dlq` | Dead-letter queue for invalid trades |
| `raw.klines.dlq` | Dead-letter queue for invalid klines |

**Example `.env`:**

```bash
SYMBOLS='["BTCUSDT", "ETHUSDT", "SOLUSDT"]'
KLINE_INTERVALS='["1m", "5m", "1h"]'
LAKE_FLUSH_SECONDS=15
```

## Batch Configuration

These control the REST backfill and silver transformation.

### Backfill Settings

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
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

**Example `.env`:**

```bash
SYMBOLS='["BTCUSDT", "ETHUSDT"]'
BACKFILL_START_DATE=2024-01-01
```

## OLAP Configuration

These control the MinIO silver → ClickHouse loader.

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `CLICKHOUSE_HOST` | `str` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `int` | `8123` | HTTP interface port |
| `CLICKHOUSE_DB` | `str` | `silver` | Target database |
| `CLICKHOUSE_USER` | `str` | `default` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | `str` | `""` | ClickHouse password |
| `CLICKHOUSE_TABLE_KLINES` | `str` | `klines_raw` | Target ClickHouse table for kline data |
| `MINIO_ENDPOINT` | `str` | `http://localhost:9000` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `str` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `str` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET_SILVER` | `str` | `silver` | Silver bucket name |
| `SILVER_KLINES_PREFIX` | `str` | `klines/` | S3 prefix under the silver bucket for kline Parquet files |

**Example `.env`:**

```bash
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DB=silver
CLICKHOUSE_TABLE_KLINES=klines_raw
```

## MinIO Storage

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `MINIO_ENDPOINT` | `str` | `http://localhost:9000` | MinIO/S3 endpoint URL |
| `MINIO_ACCESS_KEY` | `str` | `minioadmin` | S3 access key |
| `MINIO_SECRET_KEY` | `str` | `minioadmin` | S3 secret key |
| `MINIO_BUCKET_BRONZE` | `str` | `bronze` | Bronze bucket name |
| `MINIO_BUCKET_SILVER` | `str` | `silver` | Silver bucket name |
| `AWS_DEFAULT_REGION` | `str` | `us-east-1` | AWS region (required by boto3/pyarrow) |

## Docker Compose Ports

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9094 | Broker |
| Kafka UI | 8080 | Web UI |
| MinIO API | 9000 | S3 API |
| MinIO Console | 9001 | Web console |
| ClickHouse HTTP | 8123 | HTTP interface (OLAP queries, dbt) |
| ClickHouse TCP | 9009 | Native TCP (internal replication) |
| ClickHouse Prometheus | 9363 | Native metrics endpoint |
| Airflow | 8085 | Airflow webserver (LocalExecutor) |
| PostgreSQL | 5432 | Airflow metadata database |
| Prometheus | 9090 | Metrics collection + TSDB |
| Grafana | 3000 | Dashboards + alerting UI |
| Loki | 3100 | Log aggregation |
| AlertManager | 9093 | Alert routing |
| kafka-exporter | 9308 | Kafka consumer lag metrics |
| cAdvisor | 8083 | Per-container metrics |
| node-exporter | 9100 | Host hardware metrics |
| statsd-exporter | 9102 | Airflow StatsD bridge |
| Alloy | 12345 | Docker log collection |
| MLflow | 5000 | ML experiment tracking + model registry |

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

## ClickHouse Resource Limits

The ClickHouse container is configured with conservative resource limits for local development:

| Setting | Value | Source |
|---------|-------|--------|
| Container memory limit | 2 GB | `docker-compose.yaml` |
| Per-query memory cap | 256 MB | `custom-users.xml` |
| Max execution time | 60s | `custom-users.xml` |
| Max threads | 2 | `custom-users.xml` |
| Timezone | UTC | `custom-config.xml` |

## Orchestration Configuration

These control the Airflow orchestration and governance/lineage settings.

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `AIRFLOW_URL` | `str` | `http://localhost:8085` | Airflow webserver URL |
| `AIRFLOW_USER` | `str` | `airflow` | Airflow username |
| `AIRFLOW_PASSWORD` | `str` | `airflow` | Airflow password |
| `AIRFLOW_DAGS_FOLDER` | `str` | `src/orchestration/dags` | DAG definitions directory |
| `OPENLINEAGE_URL` | `str` | `http://localhost:8585/api/v1/openlineage` | OpenLineage API endpoint |
| `OPENLINEAGE_NAMESPACE` | `str` | `crypto-platform` | OpenLineage namespace identifier |
| `OPENMETADATA_URL` | `str` | `http://localhost:8585` | OpenMetadata API endpoint |
| `LINEAGE_OUTPUT_DIR` | `str` | `.exports` | Output directory for lineage manifest JSON |

**Example `.env`:**

```bash
AIRFLOW_URL=http://localhost:8085
OPENLINEAGE_NAMESPACE=crypto-platform
LINEAGE_OUTPUT_DIR=.exports
```

## ML Configuration

These control the ML feature engineering, training, and optimization pipelines.

### Feature Engineering

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `FEATURE_SYMBOLS` | `list[str]` | `["BTCUSDT"]` | Symbols to generate features for |
| `FEATURE_INTERVAL` | `str` | `1m` | Kline interval for feature data |
| `FEATURE_LOOKBACK` | `int` | `100000` | Number of historical klines to load |
| `FEATURE_OUTPUT_PATH` | `str` | `s3a://gold/features/` | Output path for feature Parquet |

### Training

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `TRAINING_EPOCHS` | `int` | `50` | Number of training epochs |
| `TRAINING_LEARNING_RATE` | `float` | `0.001` | Adam optimizer learning rate |
| `TRAINING_BATCH_SIZE` | `int` | `32` | Batch size for DataLoader |
| `TRAINING_HIDDEN_SIZE` | `int` | `128` | LSTM hidden dimension |
| `TRAINING_NUM_LAYERS` | `int` | `2` | Number of stacked LSTM layers |
| `TRAINING_DROPOUT` | `float` | `0.3` | Dropout between LSTM layers |
| `TRAINING_PATIENCE` | `int` | `10` | Early stopping patience (epochs) |

### Optimization

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `OPTIMIZATION_PRUNE_AMOUNT` | `float` | `0.30` | L1 unstructured pruning ratio (0.0–1.0) |
| `OPTIMIZATION_QUANTIZE` | `bool` | `true` | Enable dynamic INT8 quantization |
| `OPTIMIZATION_COMPILE` | `bool` | `true` | Enable `torch.compile` optimization |
| `OPTIMIZATION_EXPORT_ONNX` | `bool` | `true` | Export model to ONNX format |
| `OPTIMIZATION_BENCHMARK_SAMPLES` | `int` | `1000` | Number of forward passes for benchmarking |

### MLflow Tracking

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `MLFLOW_TRACKING_URI` | `str` | `http://localhost:5000` | MLflow tracking server URL |
| `MLFLOW_EXPERIMENT_NAME` | `str` | `crypto-lstm` | MLflow experiment name |
| `MLFLOW_S3_ENDPOINT_URL` | `str` | `http://localhost:9000` | MinIO S3 endpoint for MLflow artifacts |
