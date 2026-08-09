export AIRFLOW_HOME := env_var("HOME") + "/.airflow-project-x"
export PROJECT_ROOT := invocation_directory()
export AIRFLOW__CORE__DAGS_FOLDER := invocation_directory() + "/src/orchestration/dags"
export AIRFLOW__CORE__EXECUTOR := "LocalExecutor"
export AIRFLOW__CORE__LOAD_EXAMPLES := "false"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN := "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS := "admin:ADMIN"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE := env_var("HOME") + "/.airflow-project-x/simple_auth_passwords.json"
export AIRFLOW__API__PORT := "8085"
export AIRFLOW__API__BASE_URL := "http://localhost:8085/"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL := "http://localhost:8085/execution/"

help:
    @just --list

# ================================================================


[group('lint-check')]
pc:
    uv run pre-commit run --all-files

[group('lint-check')]
check:
    uv run ruff check . --fix --exit-non-zero-on-fix

[group('lint-check')]
format:
    uv run ruff format .

[group('lint-check')]
mypy:
    uv run mypy .

# ================================================================

[group('docs')]
docs:
    uv run mkdocs serve

# ================================================================

[group('data-pipeline')]
produce:
    uv run produce

[group('data-pipeline')]
write-lake:
    uv run write-lake

[group('data-pipeline')]
backfill:
    uv run backfill

[group('data-pipeline')]
silver:
    uv run silver

[group('data-pipeline')]
load-olap:
    uv run load-olap

[group('data-pipeline')]
stream-ohlcv:
    uv run stream-ohlcv

[group('data-pipeline')]
stream-vwap:
    uv run stream-vwap

[group('data-pipeline')]
export-lineage:
    uv run export-lineage

# ================================================================

[group('dbt')]
dbt-deps:
    cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt deps

[group('dbt')]
dbt-run:
    cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt run

[group('dbt')]
dbt-test:
    cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt test

# ================================================================

[group('ml')]
feature-eng:
    uv run feature-eng

[group('ml')]
train:
    uv run train-model

[group('ml')]
optimize:
    uv run optimize-model


# ================================================================

[group('infra')]
up profile="":
    docker compose {{ if profile == "" { "" } else { "--profile " + profile } }} up -d

[group('infra')]
down profile="":
    docker compose {{ if profile == "" { "" } else { "--profile " + profile } }} down

[group('infra')]
reload-prom:
    curl -X POST http://localhost:9090/-/reload


# ================================================================

[group('airflow')]
airflow-init:
    #!/usr/bin/env bash
    mkdir -p "$AIRFLOW_HOME"
    echo '{"admin": "admin"}' > "$AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"
    uv run airflow db migrate

[group('airflow')]
airflow-up:
    #!/usr/bin/env bash
    mkdir -p "$AIRFLOW_HOME"
    echo '{"admin": "admin"}' > "$AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"
    export PYTHONPATH="{{invocation_directory()}}/src"
    export DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true
    export CLICKHOUSE_HOST="localhost"
    export CLICKHOUSE_PORT="8123"
    export MINIO_ENDPOINT="http://localhost:9000"
    export CUBE_API_URL="http://localhost:4000"
    export MLFLOW_TRACKING_URI="http://localhost:5000"
    export AWS_ACCESS_KEY_ID="minioadmin"
    export AWS_SECRET_ACCESS_KEY="minioadmin"
    export MLFLOW_S3_ENDPOINT_URL="http://localhost:9000"
    export AIRFLOW__METRICS__STATSD_ON="true"
    export AIRFLOW__METRICS__STATSD_HOST="localhost"
    export AIRFLOW__METRICS__STATSD_PORT="8125"
    export AIRFLOW__METRICS__STATSD_PREFIX="airflow"
    uv run airflow standalone
