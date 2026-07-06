# ADR-002: confluent-kafka as the Python Kafka Client

**Date:** 2026-07-05
**Status:** Accepted

## Context

Three Python Kafka clients were evaluated:

| Library | Status | Notes |
|---|---|---|
| `kafka-python-ng` | ❌ **Archived** | Do not use |
| `kafka-python` | ⚠️ Community-pace | Pure Python; no native asyncio |
| `aiokafka` | ✅ Active | Pure Python asyncio; moderate throughput |
| **`confluent-kafka`** | ✅✅ **Chosen** | librdkafka C-based; native asyncio GA 2026; industry standard |

## Decision

Use `confluent-kafka` (≥2.15.0). For async producers, wrap blocking calls with `asyncio.get_running_loop().run_in_executor(executor, ...)` — a well-established pattern that avoids blocking the event loop while keeping full compatibility with confluent-kafka's delivery callback system.

## Consequences

| | |
|---|---|
| ✅ | Best throughput: librdkafka handles batching, compression, and retries in C |
| ✅ | Idempotent producer mode (`enable.idempotence=True`) available |
| ✅ | Industry standard — matches production deployments |
| ✅ | Python 3.13 wheels pre-built and available on PyPI |
| ⚠️ | Requires librdkafka shared library (bundled in the PyPI wheel — no manual install) |
