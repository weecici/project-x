# FAQ

## General

### What is this project?

A crypto market intelligence platform that ingests live and historical data from Binance, stores it in a lakehouse (Bronze → Silver → Gold), and eventually serves analytics via OLAP and ML price-movement predictions.

### What phase is the project in?

Phase 2 (Batch + Lake Maturation) is complete. Phases 1–2 cover data ingestion and storage. Phases 3–10 cover analytics, ML, and production deployment.

### What cryptocurrencies are supported?

Any pair listed on Binance. Defaults are BTCUSDT and ETHUSDT, but you can configure any symbol via `INGESTION_SYMBOLS` or `BACKFILL_SYMBOLS`.

## Infrastructure

### Why Kafka + MinIO instead of just a database?

The lakehouse architecture (Kafka → S3/Parquet) provides:

- **Scalability**: Parquet files scale to petabytes
- **Cost**: Object storage is cheap
- **Flexibility**: Same data serves OLAP, ML, and batch analytics
- **Durability**: Kafka provides durable buffering; MinIO provides persistent storage

### Why KRaft mode instead of ZooKeeper?

KRaft is Kafka's built-in metadata management (since Kafka 3.3). It eliminates the ZooKeeper dependency, simplifying the deployment to a single container.

### Why PySpark for batch processing?

PySpark handles:

- Large-scale deduplication and joins
- Partitioned writes to Parquet
- Hive-compatible metadata
- Local mode for development (no cluster needed)

## Configuration

### How do I add a new trading pair?

Set the `INGESTION_SYMBOLS` environment variable:

```bash
export INGESTION_SYMBOLS='["BTCUSDT", "ETHUSDT", "SOLUSDT"]'
```

Or add it to your `.env` file.

### How do I change the kline interval?

Set `INGESTION_INTERVALS`:

```bash
export INGESTION_INTERVALS='["1m", "5m", "1h"]'
```

### Where are credentials stored?

In the `.env` file (git-ignored). Default MinIO credentials are `minioadmin` / `minioadmin`. For production, use proper secrets management.

## Development

### How do I run tests?

```bash
# Unit tests (no Docker)
uv run pytest tests/unit/ -v

# Integration tests (Docker required)
uv run pytest tests/integration/ -v

# End-to-end tests (full stack)
uv run pytest tests/e2e/ -v -m e2e
```

### How do I add a new Pydantic model?

1. Define the model in the appropriate `models.py`
2. Use `ConfigDict(populate_by_name=True)` for field aliases
3. Use `Literal` types for discriminated unions
4. Write unit tests for validation

### How do I add a new CLI command?

1. Create `src/<package>/run_<command>.py`
2. Implement `main()` function
3. Register in `pyproject.toml` under `[project.scripts]`

### What pre-commit hooks are enforced?

- `ruff check` + `ruff format` (linting + formatting)
- `mypy --strict` (type checking)
- Trailing whitespace removal
- End-of-file fixer
- YAML/TOML validation

## Troubleshooting

### Kafka won't start

Check if port 9094 is already in use:

```bash
lsof -i :9094
```

### MinIO won't start

Check if ports 9000/9001 are in use:

```bash
lsof -i :9000
lsof -i :9001
```

### PySpark fails with Java error

Ensure Java 11+ is installed:

```bash
java -version
```

### Rate limiting errors (429)

The backfiller has built-in rate limiting. If you see 429 errors:

- Reduce `BACKFILL_CHUNK_DAYS` to smaller chunks
- The retry mechanism handles transient errors automatically

### Data duplication in bronze

This is by design. UUID-based filenames prevent the **lake writer** from writing duplicates, but the bronze layer may contain overlapping data from different sources. The **silver layer** handles deduplication.
