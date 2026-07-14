# ADR-010: PySpark Structured Streaming as Phase 4 Stream Processing Engine

**Date:** 2026-07-13
**Status:** Accepted

## Context

Phase 4 of the Crypto platform requires real-time streaming windowed aggregations (OHLCV, VWAP, Order Flow Imbalance, and Volatility) consuming from Kafka topics and writing to MinIO as Delta Lake tables.

The original specification planned for **Apache Flink (PyFlink)**. However:
1. **Python 3.13 Host Pinned Environment**: The workspace is strictly pinned to Python 3.13. Flink (PyFlink) does not distribute official pre-built wheels for Python 3.13 on PyPI, and building from source would incur significant developer friction and brittle setups.
2. **Container/Memory Overhead**: Flink requires running separate JobManager and TaskManager containers, which consume a minimum of ~2GB of RAM. In a resource-constrained developer environment (~7–8 GB usable RAM), this overhead increases OOM risks.
3. **Connector/JAR Management**: PyFlink requires downloading and configuring external connector JARs (`flink-sql-connector-kafka`, `flink-s3-fs-hadoop`, etc.) manually into the classpath.
4. **Cohesion**: The project already uses PySpark for Phase 2 batch silver-layer transformations, meaning PySpark 4.1.2 is already installed and verified on the local host.

## Decision

We pivot from Apache Flink to **PySpark Structured Streaming** running in `local[*]` standalone mode for Phase 4 stream processing.

PySpark Structured Streaming provides:
1. **Native Python 3.13 Compatibility**: PySpark runs flawlessly on Python 3.13, matching the local virtual environment.
2. **Zero Container Overhead**: PySpark runs locally on the host, saving ~2GB of container RAM.
3. **ACID Delta Sinks**: Delta Lake (`delta-spark`) integrates directly with Spark to support streaming writing with transactional integrity.
4. **Dynamic Package Resolution**: Spark automatically downloads Maven dependencies (`spark-sql-kafka` and `delta-spark`) on execution startup, removing the need for manual wget/curl connector JAR installation.
5. **Unified Toolchain**: Unifies data pipelines (batch + streaming) under a single framework (Apache Spark).

## Consequences

| Benefit / Trade-off | Description |
|---|---|
| ✅ **Host Compatibility** | Native execution on Python 3.13 without compile-from-source errors. |
| ✅ **Memory Savings** | ~2GB of local container RAM reclaimed; stack stays lightweight and fast. |
| ✅ **Dynamic Dependencies** | Maven packages are fetched dynamically on startup; no manual JAR file maintenance. |
| ✅ **Exactly-Once delta writes** | Handled natively via Spark checkpointing directory. |
| ⚠️ **Micro-batch Latency** | Spark processes streaming data in short micro-batches (e.g. sub-second), resulting in slightly higher latency than Flink's event-by-event model. However, for 1-minute window aggregations, sub-second latency is perfectly acceptable. |
| ⚠️ **Delta Output Mode** | Delta Lake sinks only support `append` output mode for stateful streaming aggregates, requiring us to configure both Delta and Kafka sinks in `append` mode so they emit records only when their event-time windows are finalized. |
