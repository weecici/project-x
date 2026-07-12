# System Design

Detailed breakdown of each system component, including data models, configuration, and inter-component communication.

## Component Architecture

```mermaid
graph TB
    subgraph PYTHON["Python Application"]
        direction TB
        CFG_I["IngestionConfig\nPydantic Settings"]
        CFG_B["BatchConfig\nPydantic Settings"]
        CFG_O["OlapConfig\nPydantic Settings"]

        subgraph WS_CLIENT["WebSocket Client"]
            WSM["BinanceWebSocketManager"]
            WSF["WSFeedManager"]
            WSP["TopicPartitioner"]
        end

        subgraph LAKE_WRITER["Lake Writer"]
            LWM["LakeWriterManager"]
            WCB["WriterCallback"]
            S3F["s3_factory()"]
        end

        subgraph BACKFILL["REST Backfiller"]
            BR["BinanceRestClient"]
            BF["backfill_klines()"]
        end

        subgraph SPARK["Silver Transformer"]
            KT["KlineTransformer"]
        end

        subgraph OLAP_LOADER["OLAP Loader"]
            LK["load_klines()"]
            SCHEMA["KLINES_RAW_DDL"]
        end
    end

    subgraph INFRA["Infrastructure"]
        direction TB
        KAFKA["Apache Kafka\nKRaft"]
        MINIO["MinIO S3"]
        CH["ClickHouse\nReplacingMergeTree"]
    end

    subgraph DBT["dbt Models"]
        STG["stg_crypto__klines\n(view)"]
        DAILY["fct_daily_klines\n(table)"]
        HOURLY["fct_hourly_klines\n(table)"]
        RETURNS["fct_kline_returns\n(table)"]
    end

    CFG_I --> WSM
    WSM --> WSF --> WSP
    WSP --> KAFKA
    CFG_B --> BR
    BR --> BF --> MINIO
    CFG_B --> KT
    KAFKA --> LWM
    LWM --> WCB --> S3F --> MINIO
    CFG_O --> LK
    MINIO --> LK --> CH
    SCHEMA --> CH
    CH --> STG --> DAILY
    STG --> HOURLY
    STG --> RETURNS
```

## Source Layout

```
src/
├── __init__.py
├── utils/                          # Shared cross-phase utilities
│   ├── __init__.py
│   ├── logging.py                  # Structured JSON + console logging
│   ├── retry.py                    # @async_retry decorator (exponential backoff)
│   └── storage.py                  # s3_factory() → minio.MinIO
├── ingestion/                      # Phase 1: Live data pipeline
│   ├── __init__.py
│   ├── config.py                   # IngestionConfig (Pydantic Settings)
│   ├── models.py                   # BinanceWSMessage, Trade, Kline
│   ├── run_producer.py             # Entry point: produce command
│   ├── run_lake_writer.py          # Entry point: write-lake command
│   ├── producer/
│   │   ├── __init__.py
│   │   └── ws_client.py            # WebSocket → Kafka producer
│   └── writer/
│       ├── __init__.py
│       └── lake_writer.py          # Kafka consumer → MinIO bronze writer
├── batch/                          # Phase 2: Batch processing
│   ├── __init__.py
│   ├── config.py                   # BatchConfig (Pydantic Settings)
│   ├── models.py                   # BackfillConfig, BackfillResult, SilverResult
│   ├── run_backfill.py             # Entry point: backfill command
│   ├── run_silver.py               # Entry point: silver command
│   ├── backfill/
│   │   ├── __init__.py
│   │   └── binance_rest.py         # httpx async REST client + backfill
│   └── silver/
│       ├── __init__.py
│       └── kline_transformer.py    # PySpark bronze → silver transformer
└── olap/                           # Phase 3: OLAP loading
    ├── __init__.py
    ├── config.py                   # OlapConfig (Pydantic Settings)
    ├── loader.py                   # MinIO silver → ClickHouse loader
    ├── run_loader.py               # Entry point: load-olap command
    └── schema.py                   # ClickHouse DDL (klines_raw)
```

## Data Models

### Ingestion Models (`src/ingestion/models.py`)

All models use Pydantic v2 with `model_config = ConfigDict(populate_by_name=True)`.

#### BinanceWSMessage (Union Type)

```python
BinanceWSMessage = Annotated[
    Trade | Kline,
    Field(discriminator="type")  # Discriminated on 'type' field
]
```

#### Trade

| Field | Type | Description |
|-------|------|-------------|
| `type` | `Literal["trade"]` | Always `"trade"` |
| `symbol` | `str` | Trading pair (e.g., `BTCUSDT`) |
| `trade_id` | `int` | Binance trade ID |
| `price` | `str` | Price as string (preserves precision) |
| `quantity` | `str` | Quantity as string |
| `trade_time` | `int` | Unix timestamp in milliseconds |
| `is_buyer_maker` | `bool` | Whether buyer is the maker |
| `event_time` | `int` | Event time in milliseconds |

#### Kline

| Field | Type | Description |
|-------|------|-------------|
| `type` | `Literal["kline"]` | Always `"kline"` |
| `symbol` | `str` | Trading pair |
| `interval` | `str` | Kline interval (`1m`, `5m`, `1h`, etc.) |
| `open_time` | `int` | Open time in milliseconds |
| `close_time` | `int` | Close time in milliseconds |
| `open` | `str` | Open price |
| `high` | `str` | High price |
| `low` | `str` | Low price |
| `close` | `str` | Close price |
| `volume` | `str` | Base asset volume |
| `quote_volume` | `str` | Quote asset volume |
| `trades_count` | `int` | Number of trades |
| `is_closed` | `bool` | Whether this kline is closed |

### Batch Models (`src/batch/models.py`)

#### BackfillConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `symbol` | `str` | — | Trading pair |
| `interval` | `str` | — | Kline interval |
| `start_time` | `datetime` | — | Backfill start (inclusive) |
| `end_time` | `datetime | None` | `None` | Backfill end (inclusive), None = latest |
| `limit` | `int` | `1000` | Max candles per request |
| `max_retries` | `int` | `3` | Retry count per chunk |
| `chunk_days` | `int` | `7` | Days per chunk for splitting |

#### BackfillResult

| Field | Type | Description |
|-------|------|-------------|
| `total_chunks` | `int` | Total chunks attempted |
| `success` | `int` | Successful chunks |
| `failed` | `int` | Failed chunks |
| `total_rows` | `int` | Total rows written |
| `errors` | `list[dict]` | Error details per failed chunk |

#### SilverResult

| Field | Type | Description |
|-------|------|-------------|
| `input_rows` | `int` | Rows read from bronze |
| `output_rows` | `int` | Rows written to silver |
| `duplicate_rows` | `int` | Duplicates removed |
| `execution_time_seconds` | `float` | Total execution time |
| `errors` | `list[str]` | Error messages |

### OLAP Config (`src/olap/config.py`)

#### OlapConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `clickhouse_host` | `str` | `"localhost"` | ClickHouse host |
| `clickhouse_port` | `int` | `8123` | HTTP interface port |
| `clickhouse_db` | `str` | `"silver"` | Target database |
| `clickhouse_user` | `str` | `"default"` | ClickHouse user |
| `clickhouse_password` | `str` | `""` | ClickHouse password |
| `clickhouse_table_klines` | `str` | `"klines_raw"` | Target table for kline data |
| `minio_endpoint` | `str` | `"http://localhost:9000"` | MinIO endpoint |
| `minio_access_key` | `str` | `"minioadmin"` | MinIO access key |
| `minio_secret_key` | `str` | `"minioadmin"` | MinIO secret key |
| `minio_bucket_silver` | `str` | `"silver"` | Silver bucket name |
| `silver_klines_prefix` | `str` | `"klines/"` | S3 prefix for kline Parquet |

## Configuration System

All config classes extend `pydantic_settings.BaseSettings`:

- **Env vars**: Loaded automatically (e.g., `INGESTION_SYMBOLS`, `BACKFILL_SYMBOLS`, `CLICKHOUSE_HOST`)
- **`.env` file**: Loaded via `dotenv_values()` (supports `export` prefix)
- **Defaults**: Hardcoded fallbacks in each config class
- **Type coercion**: Lists use `SettingsParameter` with a custom `__call__` for comma-separated parsing

See [Configuration Guide](../guides/configuration.md) for all available options.

## Communication Patterns

| Pattern | Components | Mechanism |
|---------|-----------|-----------|
| **Pub/Sub** | Producer → Kafka → Lake Writer | Kafka topics (`raw.trades`, `raw.klines`) |
| **Request/Response** | Backfiller → Binance API | httpx async HTTP, rate-limited (1200 req/min) |
| **Batch Read** | PySpark → MinIO | PyArrow filesystem, reads Parquet directly |
| **Object Storage** | Lake Writer → MinIO | `pyarrow.parquet.write_table()` via `minio.MinIO` |
| **Arrow Insert** | OLAP Loader → ClickHouse | `clickhouse-connect` `insert_arrow()` (zero-copy) |
| **SQL Refs** | dbt models | `ref()` and `source()` macros → ClickHouse SQL |
