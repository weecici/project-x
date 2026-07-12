pc:
    uv run pre-commit run

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

dbt-deps:
    cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt deps

dbt-run:
    cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt run

dbt-test:
    cd dbt && DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true uv run dbt test

docs:
    uv run mkdocs serve
