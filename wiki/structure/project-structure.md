
# Project structure

```
crypto-platform/
├── README.md
├── GUIDE.md                          # full 311-line project blueprint
├── docker-compose.yml                # Phase 1: Kafka + MinIO + kafka-ui
├── .env.example                      # all env vars documented
├── .pre-commit-config.yaml           # ruff + mypy hooks
├── pyproject.toml                    # deps + ruff/mypy/pytest config
├── .python-version                   # 3.13 (pinned)
│
├── ingestion/                        # Phase 1 — Binance WS → Kafka → MinIO
│   ├── config.py                     # pydantic-settings BaseSettings
│   ├── models.py                     # TradeEvent, KlineEvent (pydantic v2)
│   ├── producer/
│   │   ├── ws_client.py              # async Binance WS → confluent-kafka
│   │   └── lake_writer.py            # Kafka consumer → pyarrow → MinIO
│   ├── utils/
│   │   ├── logging.py                # loguru JSON structured logging
│   │   └── retry.py                  # tenacity async_retry decorator
│   ├── run_producer.py               # entrypoint: ws_client
│   └── run_lake_writer.py            # entrypoint: lake_writer
│
├── batch/                            # Phase 2 — Spark backfill + feature history
│   └── spark_jobs/
│
├── dbt_project/                      # Phase 3 — silver → gold SQL models
│   ├── models/silver/
│   └── models/gold/
│
├── streaming/                        # Phase 4 — Flink windowed aggregation
│   └── flink_jobs/
│
├── ml/                               # Phases 8–9 — ML pipeline and serving
│   ├── features/                     # PySpark + Numba CUDA JIT indicators
│   ├── training/                     # PyTorch LSTM/Transformer + MLflow
│   ├── optimization/                 # TorchScript, quantization, pruning
│   └── serving/
│       ├── triton_repo/              # Triton model repository (GPU)
│       ├── bento_service/            # BentoML service definition
│       └── fastapi_gateway/          # FastAPI DIY gateway
│
├── orchestration/                    # Phase 6 — Airflow DAGs
│   └── airflow_dags/
│
├── observability/                    # Phase 7 — Prometheus + Grafana
│   ├── prometheus/
│   └── grafana/dashboards/
│
├── infra/                            # Phase 10 — deployment manifests
│   ├── swarm/
│   └── k8s/
│
├── tests/                            # all phases — 3-level test suite
│   ├── conftest.py
│   ├── unit/ingestion/
│   ├── integration/
│   └── e2e/
│
└── wiki/
    ├── INDEX.md
    ├── LOG.md
    ├── architecture/
    │   ├── overview.md
    │   └── breakdown.md
    ├── decisions/                    # Architecture Decision Records
    │   ├── adr-001-kafka-kraft.md
    │   ├── adr-002-confluent-kafka.md
    │   ├── adr-003-phase1-no-flink.md
    │   └── adr-004-gpu-profile.md
    └── structure/
        ├── phase.md
        └── project-structure.md
```
