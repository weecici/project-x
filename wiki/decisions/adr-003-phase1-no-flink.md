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
