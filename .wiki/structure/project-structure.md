# Project structure

```
crypto-platform/
├── README.md
├── AGENTS.md                             # developer + LLM rules
├── docker-compose.yaml                   # Kafka + MinIO + kafka-ui + mc-init
├── .env.example                          # all env vars documented
├── .pre-commit-config.yaml               # ruff + mypy hooks
├── pyproject.toml                        # deps + ruff/mypy/pytest/mkdocs config
├── .python-version                       # 3.13 (pinned)
├── justfile                              # task runner shortcuts
├── mkdocs.yaml                           # MkDocs Material site config
│
├── src/                                  # Python package source root
│   │
│   ├── utils/                            # cross-cutting shared utilities
│   │   ├── __init__.py
│   │   ├── logging.py                    # configure_logging() — loguru JSON
│   │   ├── retry.py                      # async_retry() — tenacity backoff
│   │   └── storage.py                    # make_s3_client() — boto3 factory
│   │
│   ├── ingestion/                        # Phase 1 — Binance WS → Kafka → MinIO
│   │   ├── __init__.py
│   │   ├── config.py                     # IngestionConfig (pydantic-settings)
│   │   ├── models.py                     # TradeEvent, KlineEvent (pydantic v2)
│   │   ├── producer/
│   │   │   └── ws_client.py              # async Binance WS → confluent-kafka
│   │   ├── writer/
│   │   │   └── lake_writer.py            # Kafka consumer → pyarrow → MinIO bronze
│   │   ├── run_producer.py               # entrypoint: uv run produce
│   │   └── run_lake_writer.py            # entrypoint: uv run write-lake
│   │
│   ├── batch/                            # Phase 2 — REST backfill + PySpark silver
│   │   ├── __init__.py
│   │   ├── config.py                     # BatchConfig (pydantic-settings)
│   │   ├── models.py                     # KlineRow (from_api_list classmethod)
│   │   ├── backfill/
│   │   │   └── binance_rest.py           # paginated REST → bronze Parquet
│   │   ├── silver/
│   │   │   └── kline_transformer.py      # PySpark: dedup + cast + partition → silver
│   │   ├── run_backfill.py               # entrypoint: uv run backfill
│   │   └── run_silver.py                 # entrypoint: uv run silver
│   │
│   ├── streaming/                        # Phase 4 — Flink windowed aggregation
│   │   └── flink_jobs/
│   │
│   ├── ml/                               # Phases 8–9 — ML pipeline and serving
│   │   ├── features/
│   │   ├── training/
│   │   ├── optimization/
│   │   └── serving/
│   │       ├── triton_repo/
│   │       ├── bento_service/
│   │       └── fastapi_gateway/
│   │
│   └── orchestration/                    # Phase 6 — Airflow DAGs
│       └── airflow_dags/
│
├── dbt_project/                          # Phase 3 — silver → gold SQL models
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── silver/
│       └── gold/
│
├── infra/                                # Ops, Deployment & Observability
│   ├── swarm/                            # Docker Swarm configurations
│   ├── k8s/                              # Kubernetes manifests
│   ├── observability/                    # Prometheus + Grafana
│   │   ├── prometheus/
│   │   └── grafana/dashboards/
│   └── metadata/                         # OpenMetadata lineage configs
│
├── docs/                                 # MkDocs source (mkdocstrings auto-API)
│   ├── index.md
│   ├── architecture/
│   ├── getting-started/
│   └── guides/
│
├── tests/                                # 3-level test suite
│   ├── conftest.py                       # shared fixtures
│   ├── unit/
│   │   ├── utils/
│   │   │   └── test_retry.py
│   │   ├── ingestion/
│   │   │   ├── test_config.py
│   │   │   └── test_models.py
│   │   └── batch/
│   │       ├── test_batch_config.py
│   │       ├── test_batch_models.py
│   │       └── test_binance_rest.py
│   ├── integration/
│   │   ├── test_kafka_roundtrip.py
│   │   ├── test_minio_writer.py
│   │   └── test_silver_spark.py
│   └── e2e/
│       ├── test_phase1_pipeline.py
│       └── test_phase2_backfill.py
│
└── .wiki/                                # LLM-owned wiki documentation
    ├── INDEX.md
    ├── LOG.md
    ├── decisions/
    │   ├── adr-001-kafka-kraft.md
    │   ├── adr-002-confluent-kafka.md
    │   ├── adr-003-phase1-no-flink.md
    │   ├── adr-004-gpu-profile.md
    │   ├── adr-005-src-layout.md
    │   ├── adr-006-utils-package.md
    │   ├── adr-007-pyspark-local-mode.md
    │   └── adr-008-httpx-rest-client.md
    └── structure/
        ├── phase.md
        └── project-structure.md
```
