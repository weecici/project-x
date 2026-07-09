# API — Ingestion

Live data ingestion from Binance WebSocket to Kafka and MinIO.

## Overview

The `ingestion` package handles real-time crypto data:

- **Models** — Typed Pydantic models for Binance WebSocket messages
- **Config** — Settings for symbols, intervals, Kafka, and flush behavior
- **WebSocket Client** — Connects to Binance, publishes to Kafka
- **Lake Writer** — Consumes from Kafka, writes Parquet to MinIO

::: ingestion.config
    options:
      show_source: true
      members_order: source

::: ingestion.models
    options:
      show_source: true
      members_order: source

::: ingestion.producer.ws_client
    options:
      show_source: true
      members_order: source

::: ingestion.writer.lake_writer
    options:
      show_source: true
      members_order: source
