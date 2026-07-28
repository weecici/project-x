pc:
    uv run pre-commit run

up:
  docker compose up -d

down:
  docker compose down

check:
    uv run ruff check .

format:
    uv run ruff format .

mypy:
    uv run mypy .

produce:
    uv run produce

write-lake:
    uv run write-lake

backfill:
    uv run backfill

silver:
    uv run silver

load-olap:
    uv run load-olap

stream-ohlcv:
    uv run stream-ohlcv

stream-vwap:
    uv run stream-vwap

dbt-deps:
    cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt deps

dbt-run:
    cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt run

dbt-test:
    cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt test

docs:
    uv run mkdocs serve

export-lineage:
    uv run export-lineage

airflow-pass:
    docker compose logs airflow | grep "Password for user" | awk '{print $NF}'
