# API — Utils

Shared utilities used across all phases of the platform.

## Overview

The `utils` package provides foundational infrastructure:

- **Logging** — Structured JSON and console logging setup
- **Retry** — Async decorator with exponential backoff
- **Storage** — S3/MinIO client factory

::: utils.logging
    options:
      show_source: true
      members_order: source

::: utils.retry
    options:
      show_source: true
      members_order: source

::: utils.storage
    options:
      show_source: true
      members_order: source
