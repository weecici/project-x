# Project structure

```
crypto-platform/
├── README.md
├── AGENTS.md                             # developer + LLM rules
├── docker-compose.yaml                   # Kafka + MinIO + ClickHouse + kafka-ui + mc-init
├── .env / .env.example                   # all env vars (Phases 1–3 documented)
├── .pre-commit-config.yaml               # ruff + mypy hooks
├── pyproject.toml                        # deps + ruff/mypy/pytest/mkdocs config
├── .python-version                       # 3.13 (pinned)
├── justfile                              # task runner shortcuts
├── mkdocs.yaml                           # MkDocs Material site config
├── cube/                                 # Phase 5 — Cube semantic layer configurations
│   ├── cube.js                           # global timer/refresh configs
│   └── model/
│       ├── cubes/
│       │   └── crypto/
│       │       ├── daily_klines.yml      # daily klines cube
│       │       ├── hourly_klines.yml     # hourly klines cube
│       │       └── kline_returns.yml     # kline returns cube
│       └── views/
│           └── crypto/
│               ├── ohlcv_daily.yml       # daily OHLCV view exposed to BI
│               ├── ohlcv_hourly.yml      # hourly OHLCV view exposed to BI
│               └── price_analytics.yml   # volatility/return view exposed to BI
│
├── src/                                  # Python package source root (namespace package)
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
│   ├── olap/                             # OLAP Layer (loader + semantic exporter)
│   │   ├── __init__.py
│   │   ├── config.py                     # OlapLoaderConfig + BiExporterConfig
│   │   ├── loader.py                     # Silver Parquet → ClickHouse loader + CLI
│   │   └── exporter.py                   # Cube REST API client → CSV / Google Sheets sync + CLI
│   │
│   ├── streaming/                        # Phase 4 — PySpark windowed aggregation
│   │   ├── __init__.py
│   │   ├── config.py                     # StreamingConfig (pydantic-settings)
│   │   ├── run_ohlcv.py                  # CLI entrypoint for OHLCV stream
│   │   ├── run_vwap.py                   # CLI entrypoint for VWAP / metrics stream
│   │   └── jobs/
│   │       ├── __init__.py
│   │       ├── ohlcv_stream.py           # Spark structured streaming job for OHLCV
│   │       └── vwap_stream.py            # Spark structured streaming job for VWAP / microstructure metrics
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
│   └── orchestration/                    # Phase 6 — Airflow Orchestration & OpenMetadata Governance
│       ├── __init__.py
│       ├── config.py                     # OrchestrationConfig & GovernanceConfig
│       ├── dags/                         # Airflow DAGs (LocalExecutor)
│       │   ├── crypto_batch_backfill_dag.py
│       │   ├── crypto_olap_serving_dag.py
│       │   └── crypto_ml_retrain_dag.py
│       └── governance/                   # OpenLineage & OpenMetadata manifest compiler
│           ├── lineage.py
│           └── run_lineage.py            # CLI entrypoint: export-lineage
│
├── dbt/                                  # Phase 3 — silver → gold SQL models
│   ├── dbt_project.yml
│   ├── profiles.yml                      # ClickHouse HTTP connection (reads env vars)
│   ├── packages.yml                      # dbt_utils
│   ├── .gitignore                        # target/, dbt_packages/, logs/
│   ├── macros/
│   │   └── generate_schema_name.sql      # overrides target suffix, flat silver/gold DB mapping
│   └── models/
│       ├── staging/
│       │   └── crypto/
│       │       ├── sources.yml           # silver.klines_raw source declaration
│       │       ├── staging.yml           # docs + column tests for staging views
│       │       └── stg_crypto__klines.sql # typed view inside 'silver' database (uses FINAL)
│       └── marts/
│           ├── marts.yml                 # docs + expression tests for gold fact tables
│           ├── fct_daily_klines.sql      # daily aggregated OHLCV (argMin/argMax)
│           ├── fct_hourly_klines.sql     # hourly aggregated OHLCV (toStartOfHour)
│           └── fct_kline_returns.sql     # log returns (standard lag() window function)
│
├── infra/
│   ├── clickhouse/
│   │   ├── config.d/
│   │   │   └── custom-config.xml         # timezone, listen interface, log overrides
│   │   └── users.d/
│   │       └── custom-users.xml          # default DB gold, 256MB query memory caps
│   ├── swarm/                            # Phase 10 — Docker Swarm configs
│   ├── k8s/                              # Phase 10 — Kubernetes manifests
│   ├── observability/                    # Phase 7 — Prometheus + Grafana
│   │   ├── prometheus/
│   │   └── grafana/dashboards/
│   └── metadata/                         # Phase 6 — OpenMetadata lineage configs
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
│   │   ├── batch/
│   │   │   ├── test_batch_config.py
│   │   │   ├── test_batch_models.py
│   │   │   └── test_binance_rest.py
│   │   └── olap/
│   │       └── test_olap_config.py
│   │   └── semantic/
│   │       └── test_semantic_config.py
│   ├── integration/
│   │   ├── test_kafka_roundtrip.py
│   │   ├── test_minio_writer.py
│   │   ├── test_silver_spark.py
│   │   ├── test_olap_loader.py
│   │   └── test_bi_export.py
│   └── e2e/
│       ├── test_phase1_pipeline.py
│       └── test_phase2_backfill.py
│
└── .agents/                              # LLM-owned customization root
    ├── skills/
    │   └── git-commit/
    └── wiki/
        ├── INDEX.md
        ├── LOG.md
        ├── architecture/
        │   ├── overview.md
        │   └── breakdown.md
        ├── decisions/
        │   ├── adr-001-kafka-kraft.md
        │   ├── adr-002-confluent-kafka.md
        │   ├── adr-003-phase1-no-flink.md
        │   ├── adr-004-gpu-profile.md
        │   ├── adr-005-src-layout.md
        │   ├── adr-006-utils-package.md
        │   ├── adr-007-pyspark-local-mode.md
        │   ├── adr-008-httpx-rest-client.md
        │   ├── adr-009-clickhouse-olap.md
        │   ├── adr-010-pyspark-structured-streaming.md
        │   └── adr-011-cube-semantic-layer.md
        └── structure/
            ├── phase.md
            └── project-structure.md
```
