pc:
    pre-commit run

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

docs:
    uv run mkdocs serve
