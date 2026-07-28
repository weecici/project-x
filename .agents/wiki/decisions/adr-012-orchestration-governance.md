# ADR-012: Apache Airflow & OpenMetadata for Orchestration and Governance

## Status

Accepted

## Context

Phase 6 requires establishing a workflow orchestration engine and data governance framework for our crypto analytics platform. The system must manage dependencies between batch historical backfills, PySpark silver lake transformations, ClickHouse OLAP loading, dbt gold SQL model transformations, semantic BI exports, and ML retraining sensors.

Furthermore, end-to-end data lineage must be captured across all medallion architecture tiers (Kafka → MinIO → Spark → ClickHouse → dbt → Cube → BI exports) to provide complete governance transparency.

## Decision

1. **Workflow Orchestration**:
   - We adopt **Apache Airflow 3.3.0** (`apache/airflow:latest` with Python 3.13) running in `LocalExecutor` mode backed by a lightweight PostgreSQL metadata database (`postgres:alpine`, capped at 256MB RAM).
   - Airflow host webserver is exposed on port `8085` (mapped from container port `8080` to prevent collisions with Kafka UI on `8080`).
   - DAGs follow Astronomer and Airflow domain-driven naming conventions matching `dag_id` to python filename:
     - `crypto_batch_backfill_dag.py` (`crypto_batch_backfill`) — utilizes Dynamic Task Mapping (`.expand()`) over symbols (`BTCUSDT`, `ETHUSDT`) for parallel REST backfilling.
     - `crypto_olap_serving_dag.py` (`crypto_olap_serving`)
     - `crypto_ml_retrain_dag.py` (`crypto_ml_retrain`)

2. **Data Governance & Lineage**:
   - Standardize on **OpenLineage** runtime event emission (`apache-airflow-providers-openlineage`).
   - Implement an in-repo Lineage Manifest Compiler (`src/orchestration/governance/lineage.py`) exposing the `uv run export-lineage` CLI command.
   - Generates OpenMetadata compliant JSON manifest graph (`/tmp/exports/lineage_manifest.json`) tracking directed data flow edges across all 5 architecture layers.

## Rationale

1. **Resource Constraint Compliance**: Running Airflow LocalExecutor in a single container with a lightweight Alpine PostgreSQL database keeps memory footprint under ~1 GB RAM, avoiding out-of-memory errors on 7–8 GB usable RAM development hardware.
2. **PostgreSQL vs SQLite**: Airflow's built-in SQLite backend is restricted to `SequentialExecutor` (single-task serial execution) and file-level database write locks. PostgreSQL enables concurrent process execution across worker tasks.
3. **Domain-Driven DAG Architecture**: Structuring DAGs by business domain rather than numerical indices ensures clarity, maintainability, and alignment with modern data engineering standards.

## Consequences

- Added `postgres` and `airflow` services to `docker-compose.yml`.
- Created package `src/orchestration/` containing `config.py`, `dags/`, and `governance/`.
- Registered `export-lineage` script in `pyproject.toml`.
- Added unit and integration tests validating DAG structure, DAG loading, and lineage manifest generation.
