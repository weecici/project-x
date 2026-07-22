# API — Streaming

Real-time stream processing using PySpark Structured Streaming.

## Overview

The `streaming` package ingests trades and klines from Kafka and writes outputs to Delta Lake and Kafka.

- **Config** — Kafka, MinIO, Spark, and streaming settings
- **OHLCV Stream** — Filters closed kline bars, casts to Silver types, dual-sinks to Delta + Kafka
- **VWAP Stream** — Computes VWAP, OFI, volatility, and trade count from trades via tumbling windows with event-time watermarking

::: streaming.config
    options:
      show_source: true
      members_order: source

::: streaming.jobs.ohlcv_stream
    options:
      show_source: true
      members_order: source

::: streaming.jobs.vwap_stream
    options:
      show_source: true
      members_order: source

::: streaming.run_ohlcv
    options:
      show_source: true
      members_order: source

::: streaming.run_vwap
    options:
      show_source: true
      members_order: source
