# System Design

Detailed breakdown of each system component, including data models, configuration, and inter-component communication.

## Component Architecture

```mermaid
graph TB
    subgraph PYTHON["Python Application"]
        direction TB
        CFG_I["IngestionConfig\nPydantic Settings"]
        CFG_B["BatchConfig\nPydantic Settings"]

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
    end

    subgraph INFRA["Infrastructure"]
        direction TB
        KAFKA["Apache Kafka\nKRaft"]
        MINIO["MinIO S3"]
    end

    CFG_I --> WSM
    WSM --> WSF --> WSP
    WSP --> KAFKA
    CFG_B --> BR
    BR --> BF --> MINIO
    CFG_B --> KT
    KAFKA --> LWM
    LWM --> WCB --> S3F --> MINIO
```

## Source Layout

```
src/
├── __init__.py
├── utils/                  # Shared cross-phase utilities
│   ├── __init__.py         # Public API re-exports
│   ├── config.py           # load_dotenv(), get_log_level()
│   ├── logging.py          # Structured JSON + console logging
│   ├── retry.py            # @async_retry decorator (exponential backoff)
│   └── storage.py          # s3_factory() → minio.MinIO
├── ingestion/              # Phase 1: Live data pipeline
│   ├── __init__.py
│   ├── config.py           # IngestionConfig (Pydantic Settings)
│   ├── models.py           # BinanceWSMessage, Trade, Kline, etc.
│   ├── ws_client.py        # WebSocket → Kafka producer
│   ├── lake_writer.py      # Kafka consumer → MinIO bronze writer
│   ├── run_producer.py     # Entry point: produce command
│   └── run_lake_writer.py  # Entry point: write-lake command
└── batch/                  # Phase 2: Batch processing
    ├── __init__.py
    ├── config.py           # BatchConfig (Pydantic Settings)
    ├── models.py           # BackfillConfig, BackfillResult, etc.
    ├── binance_rest.py     # httpx async REST client + backfill
    ├── kline_transformer.py # PySpark bronze → silver transformer
    ├── run_backfill.py     # Entry point: backfill command
    └── run_silver.py       # Entry point: silver command
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

## Configuration System

Both `IngestionConfig` and `BatchConfig` extend `pydantic_settings.BaseSettings`:

- **Env vars**: Loaded automatically (e.g., `INGESTION_SYMBOLS`, `BACKFILL_SYMBOLS`)
- **`.env` file**: Loaded via `dotenv_values()` (supports `export` prefix)
- **Defaults**: Hardcoded fallbacks in the `Settings` inner class
- **Type coercion**: Lists use `SettingsParameter` with a custom `__call__` for comma-separated parsing

See [Configuration Guide](../guides/configuration.md) for all available options.

## Communication Patterns

| Pattern | Components | Mechanism |
|---------|-----------|-----------|
| **Pub/Sub** | Producer → Kafka → Lake Writer | Kafka topics (`raw.trades`, `raw.klines`) |
| **Request/Response** | Backfiller → Binance API | httpx async HTTP, rate-limited (1200 req/min) |
| **Batch Read** | PySpark → MinIO | PyArrow filesystem, reads Parquet directly |
| **Object Storage** | Lake Writer → MinIO | `pyarrow.parquet.write_table()` via `minio.MinIO` |
