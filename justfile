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
