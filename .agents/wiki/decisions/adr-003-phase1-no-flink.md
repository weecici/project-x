# ADR-003: Defer Apache Flink to Phase 4

**Date:** 2026-07-05
**Status:** Accepted

## Context

Phase 1 goal: a running, observable pipeline where Binance WS data lands in MinIO bronze. The original plan placed Flink in Phase 1. However:

1. Flink Job Manager + Task Manager (1 TM) require at least 1 GB RAM when memory is properly configured — significant on a 7–8 GB usable RAM machine already running Kafka + MinIO.
2. Flink in Phase 1 would require configuring S3A/MinIO connector JARs, checkpoint storage, and state backend before any other component is validated.
3. Phase 1 can fully achieve its deliverable without Flink: the Python `LakeWriter` process consumes from Kafka and writes Parquet directly to MinIO.

## Decision

Flink is deferred to Phase 4, which is dedicated to stream processing. In Phases 1–3, the `LakeWriter` Python process handles Kafka → MinIO writes. Delta Lake is enabled starting in Phase 4 when Flink comes online and adds ACID semantics.

## Consequences

| | |
|---|---|
| ✅ | Phase 1 is simpler, faster to deliver, and fully testable |
| ✅ | Flink gets a dedicated phase with proper memory config and connector setup |
| ✅ | Python LakeWriter is independently unit-testable without JVM services |
| ⚠️ | Bronze files in Phase 1 lack event-time windowing — they are raw append-only dumps |
| ⚠️ | Flink is introduced later; don't mistake this for "we're skipping Flink" |

---

## Status Update (2026-07-13)

Upon entering Phase 4, the platform team pivoted from Apache Flink to **PySpark Structured Streaming** for real-time aggregation. The primary driver was Python 3.13 host compatibility (PyFlink lacks official PyPI wheels for 3.13), along with the benefit of reclaiming ~2GB of container overhead by running Spark streaming in local mode alongside the existing Spark installation. Delta Lake transaction logs and stream processing are fully implemented via PySpark.
