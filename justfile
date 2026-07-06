pc:
    pre-commit run --all-files

producer:
    PYTHONPATH=. uv run ingestion/run_producer.py

lake-writer:
    PYTHONPATH=. uv run ingestion/run_lake_writer.py
