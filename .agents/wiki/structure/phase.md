# Phased build order

| # | Name | Active Services | Deliverable |
|---|---|---|---|
| **1** | **Foundation + Ingestion** | Kafka (KRaft), MinIO, kafka-ui | WS producer → Kafka → bronze Parquet in MinIO; pytest suite green |
| **2** | **Batch + Lake Maturation** | Kafka, MinIO, PySpark standalone | REST historical backfill → bronze; PySpark silver (dedup, schema, Parquet partitioning) |
| **3** | **OLAP + dbt** | ClickHouse, MinIO, dbt | dbt silver→gold models; queryable ClickHouse; dbt tests passing |
| **4** | **Stream Processing** | Kafka, Flink (JM + 1 TM), MinIO | Flink OHLCV/VWAP windowed aggregates → silver; Delta Lake enabled |
| **5** | **Semantic Layer + BI** | ClickHouse, Cube.js | Cube metrics API on ClickHouse; Tableau Public dashboard; Excel sanity export |
| **6** | **Orchestration + Governance** | Airflow (LocalExecutor), OpenMetadata | Airflow DAGs for batch + dbt + retrain; full lineage graph in OpenMetadata |
| **7** | **Observability** | Prometheus, Grafana | Infra health dashboard; pre-wired ML serving dashboard |
| **8** | **ML Pipeline + Optimization** | MinIO, MLflow, PySpark, PyTorch (CUDA sm_86) | Feature eng (PySpark + Numba CUDA JIT); model training on GPU; MLflow registry; TorchScript + quantization + pruning benchmark table |
| **9** | **ML Serving (3-way)** | Triton (GPU), BentoML, FastAPI, Locust | All 3 serving paths live; Locust load-test; latency/throughput in Grafana |
| **10** | **CI/CD + Deploy + Polish** | GitHub Actions, Docker Swarm | GH Actions pipeline; Swarm MVP; K8s manifests; README; ADRs; Doris comparison note; demo GIF |

> See `.wiki/decisions/` for Architecture Decision Records explaining key choices.
