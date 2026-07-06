
# Component-by-component breakdown & why each tool

## 1. Ingestion — Apache Kafka
A Python producer subscribes to the Binance WebSocket and publishes raw trade/kline events into two Kafka topics (`raw.trades`, `raw.klines`). This is your "always-on" real-time backbone — everything downstream that needs freshness reads from Kafka rather than hitting the exchange directly.

## 2. Stream processing — Apache Flink
Flink consumes from Kafka and computes **true event-time windowed aggregates**: rolling VWAP, 1s/1m/5m OHLCV bars, order-flow imbalance, trade-rate spikes. This is the correct tool for this job specifically because it gives you genuine event-time semantics and stateful streaming — using Spark Structured Streaming here instead would be a defensible alternative, but Flink is the stronger CV signal for "I understand real streaming, not micro-batch."

Flink writes its aggregated output back into Kafka (`agg.klines`) and/or directly to the lake as append-only files.

## 3. Batch processing — Spark / PySpark (+ optional Databricks)
Spark (via PySpark) does two jobs:
1. **Historical backfill** — pulls months of history from the Binance REST klines endpoint and lands it in the lake, so your gold layer isn't just "whatever's arrived since I started the stream."
2. **Feature history recomputation** — periodically recomputes longer-window technical indicators (20/50/200-period moving averages, RSI, Bollinger Bands) over the full history, which is naturally a batch job, not a streaming one.

**Optional stretch:** replicate the backfill job as a notebook on **Databricks Community Edition** (free, real Databricks environment, Delta Lake native). This lets you honestly list Databricks on your CV with something concrete you built in it, without needing a paid workspace.

## 4 Lake storage — S3 / MinIO, medallion architecture
- **Bronze**: raw Kafka/Flink dumps and raw REST pulls, as-is, append-only.
- **Silver**: cleaned, deduplicated, schema-enforced Parquet (or Delta/Iceberg tables if you want to also learn table-format transaction semantics — genuinely worth it, and a strong resume line).
- **Gold**: business-ready aggregates (per-symbol daily/hourly OHLCV, feature tables) ready for dbt to pick up.

Locally, run **MinIO** (S3-compatible, self-hosted, free) so you never need an AWS bill during development; swap the endpoint for real S3 only if/when you want to demo cloud deployment.

## 5 Transformation — dbt
dbt sits on top of your warehouse (ClickHouse) and owns the silver→gold SQL transformations, with tests (`not_null`, `unique`, freshness checks) and auto-generated documentation. This is also your cleanest lineage source, since dbt's lineage graph feeds directly into OpenLineage/OpenMetadata.

## 6 OLAP serving — ClickHouse (primary) + Apache Doris (comparison)
Use **ClickHouse** as your primary analytical store — it's the most widely recognized real-time OLAP engine on the market right now and pairs cleanly with dbt and Cube. Since you already have **Doris** experience from your day job, stand up a **secondary Doris instance** serving the same gold tables and write a short comparison note (ingestion model, materialized view behavior, query latency on your workload) in your README. That's a much stronger CV signal than just listing both — it shows you can reason about *why* you'd pick one OLAP engine over another, which is a real interview question.

## 7 Semantic layer / BI — Cube + Tableau Public
**Cube** sits on ClickHouse and defines your metrics once (e.g. "hourly return," "volatility," "trade volume") so every downstream consumer (a dashboard, an API, a notebook) uses the same definition — this is the actual point of a semantic layer, and it's a much more current concept than a raw BI tool.

For the classic BI skill line on your CV, export one gold-layer table as CSV and build a **Tableau Public** dashboard (free, publicly shareable link — genuinely reachable and demoable). Do a quick Excel pivot-table pass on the same export as a "sanity check" artifact — it's cheap to add and covers that line item honestly.

## 8 Orchestration — Airflow
Airflow owns:
- The nightly Spark backfill/recompute DAG
- The dbt run/test DAG
- The MLflow retraining DAG (weekly retrain on latest features)
- Sensors/triggers between them (e.g. don't retrain until dbt tests pass)

## 9 Metadata & lineage — OpenMetadata + OpenLineage
Install the `apache-airflow-providers-openlineage` package so Airflow emits lineage events automatically; dbt also emits OpenLineage events natively. Point both at **OpenMetadata**, which ingests OpenLineage events and gives you a searchable catalog with an actual end-to-end lineage graph (Kafka topic → Spark job → lake table → dbt model → ClickHouse table). This is the single highest-leverage addition for making your project look like it was built by someone who has worked on a real data platform team, not a tutorial follower — very few personal projects bother with data governance at all.

## 10 Observability — Prometheus + Grafana
Scrape metrics from Kafka, Flink, Spark, ClickHouse, and your model-serving containers. Build two Grafana dashboards:
1. **Infra health** — consumer lag, Flink checkpoint duration, ClickHouse query latency.
2. **ML serving health** — request rate, p50/p95/p99 latency, and error rate per serving backend (Triton vs BentoML vs FastAPI) — this is what actually demonstrates MLOps monitoring maturity, not just "I put Grafana in front of a database."

## 11 Feature engineering — PySpark + Numba
Compute standard technical indicators (RSI, MACD, Bollinger Bands, rolling volatility) in PySpark for the bulk historical pass. Then implement the same indicators as a **hand-written, loop-based Python function accelerated with Numba's `@jit(nopython=True)`**, and benchmark it against a naive pure-Python version and against the Spark version. This gives you a genuine, honest before/after speedup number for your CV ("Xx speedup via Numba JIT on custom feature computation") instead of a vague claim.

## 12 Model training & tracking — PyTorch + MLflow
Train a small sequence model (a compact LSTM or a small Transformer encoder — a few layers is plenty here, this is a learning project not a research one) to predict short-horizon price direction from the engineered features. Log every run — hyperparameters, metrics, model artifacts — to **MLflow's tracking server**, and use the **MLflow Model Registry** to promote your best run to "staging" → "production."

Be explicit in your README that accuracy on this task will likely hover only modestly above a coin-flip baseline (that's expected and honest for near-efficient markets) — what you're demonstrating is the *pipeline*, not alpha.

## 13 Model optimization — TorchScript, quantization, pruning
For your best MLflow-registered model:
1. Export to **TorchScript** (`torch.jit.script` or `.trace`) for a Python-free, optimized runtime graph.
2. Apply **dynamic quantization** (`torch.quantization.quantize_dynamic`) and measure size/latency change.
3. Apply **structured or unstructured pruning** (`torch.nn.utils.prune`) and measure the accuracy/latency trade-off.

Record a small table in your README: baseline vs TorchScript vs quantized vs pruned — size (MB), p95 latency (ms), accuracy. This table alone is a strong, concrete MLOps artifact.

## 14 Model serving — three-way comparison
Serve the optimized model through **all three** of these, deliberately, so you can write a comparison:
- **Triton Inference Server** — production-grade, dynamic batching, concurrent model execution; the industry-standard choice when serving needs real throughput.
- **BentoML** — Python-native packaging, faster to stand up, good developer experience, weaker on extreme low-latency GPU serving than Triton.
- **FastAPI (DIY)** — a hand-rolled endpoint loading the TorchScript model directly, as the baseline everyone compares against.

Load-test all three with the same tool (e.g. `locust` or `wrk`) and put latency/throughput numbers in Grafana (see 10.). This "I built it three ways and can tell you why you'd pick each one" story is far more interview-defensible than "I used Triton" alone.

FastAPI additionally serves as the outward-facing gateway — one clean REST API (with auto-generated OpenAPI docs) that internally calls whichever serving backend is active, so external consumers (your dashboard, a demo script) never need to know which backend is live.

## 15 CI/CD — GitHub Actions
- On PR: run dbt tests, run unit tests for the feature-engineering functions, lint.
- On merge to main: build and push Docker images for each service, run the load test suite against a staging deploy.
- On tag: deploy to your Kubernetes cluster (or Swarm, in the MVP phase).

## 16 Containerization — Docker Compose → Docker Swarm → Kubernetes
Don't pick one and skip the others — sequence them as your actual build order:
1. **Docker Compose** for local development (Kafka, ClickHouse, MinIO, Airflow, Grafana/Prometheus, MLflow all as compose services). This is where you'll spend most of your time.
2. **Docker Swarm** for your first "multi-node-style" deploy — simpler than K8s, good for proving the MVP runs outside your laptop.
3. **Kubernetes** (via `k3s` or `kind` locally, or a small managed cluster) for the version you actually put on your CV as the production deployment target — this is the industry-standard expectation for the role you're targeting, so it should be the one you can speak to in depth in an interview, with Swarm mentioned as "I also built and compared a simpler Swarm deployment first."

## 17 On-premise vs serverless — make this an explicit architecture decision, not just a checkbox
Write a short "Architecture Decision Record" in your repo:
- **Core platform (Kafka, Flink, Spark, ClickHouse, Airflow):** self-hosted via Docker/K8s on your own machine or a cheap VPS. Rationale: full control, no vendor lock-in, and it's the only way to actually demonstrate on-prem/self-managed infra skills.
- **One genuinely serverless component:** e.g. a lightweight alerting function (price-spike → notification) deployed as an AWS Lambda / GCP Cloud Function behind an API Gateway. Rationale: demonstrates you understand *when* serverless is the right call (bursty, low-frequency, stateless work) rather than defaulting to it everywhere.

This single paragraph in your README does more for a hiring manager than either choice alone would.
