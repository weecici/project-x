# API — Batch

Historical data backfill and bronze → silver transformation.

## Overview

The `batch` package handles batch processing:

- **Models** — Data models for backfill config and results
- **Config** — Settings for symbols, date ranges, and silver paths
- **Binance REST Client** — Async HTTP client with rate limiting
- **Kline Transformer** — PySpark job for bronze → silver dedup and cleaning

::: batch.config
    options:
      show_source: true
      members_order: source

::: batch.models
    options:
      show_source: true
      members_order: source

::: batch.backfill.binance_rest
    options:
      show_source: true
      members_order: source

::: batch.silver.kline_transformer
    options:
      show_source: true
      members_order: source
