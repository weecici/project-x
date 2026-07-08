# ADR-007: PySpark `local[*]` Mode for Batch Silver Transformations

## Status

Accepted

## Context

Phase 2 requires transforming bronze kline Parquet into a typed, deduplicated silver
layer. The transformation involves schema enforcement, Decimal casting, window-based
deduplication, and partitioned writes — a natural fit for a distributed DataFrame API.

The machine constraint is significant: **~7–8 GB usable RAM** with IDE and browser
running. A traditional PySpark cluster (separate JM + TM containers) or a Spark
Standalone cluster (master + worker) would consume an additional 1–2 GB for the
cluster management layer.

## Decision

Run PySpark in **`local[*]`** mode — driver and executor in the same JVM process on
the host. No cluster, no additional containers.

```python
SparkSession.builder.master("local[*]") ...
```

Memory budget:
- `spark.driver.memory = 1g` (configurable via `SPARK_DRIVER_MEMORY`)
- `spark.executor.memory = 1g` (configurable via `SPARK_EXECUTOR_MEMORY`)
- Total JVM overhead: **~2.5 GB** — within the 7–8 GB usable window

## Why Not a Cluster / Docker Container

| Option | RAM overhead | Verdict |
|---|---|---|
| `local[*]` | ~2.5 GB total | ✅ Fits |
| Spark Standalone (master + 1 worker) | +512 MB extra | ⚠️ Tight |
| `docker compose` Spark service | +1 GB extra | ❌ Too tight |
| Databricks Community Edition | External SaaS | Out of scope for local dev |

For a portfolio-scale backfill (months of OHLCV data for 2 symbols), `local[*]`
is not a limitation — it processes all data in a single pass with full parallelism
across all 16 available CPU cores.

## Delta Lake Deferral

Delta Lake (`delta-spark`) is **not** introduced in Phase 2. Reasons:

1. Delta adds ACID semantics and time-travel — features that have value only when
   multiple writers exist simultaneously (Phase 4: Flink + Spark concurrent writes).
2. Adding `delta-spark` increases PySpark startup time and JAR resolution complexity
   with no benefit in a Parquet-only Phase 2 pipeline.
3. Delta is introduced in Phase 4 as part of the Flink streaming work.

## Consequences

- No new Docker Compose services needed for Phase 2.
- Silver transformer is invoked via `uv run silver` — PySpark starts, runs, and stops.
- Memory knobs (`SPARK_DRIVER_MEMORY`, `SPARK_EXECUTOR_MEMORY`) are env-var controlled.
- Phase 4 will add Delta Lake; the silver write path in `kline_transformer.py` will be
  updated to `format("delta")` at that point.
