"""Airflow DAG for OLAP silver loading, dbt transformations, and BI metrics export.

Loads silver Parquet files into ClickHouse, runs dbt gold SQL models, enforces dbt
quality assertions, and exports metrics via Cube REST API / Google Sheets.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

from olap.exporter import main as export_bi
from olap.loader import main as load_olap
from orchestration.governance.run_lineage import main as export_lineage

silver_klines_asset = Asset("s3://silver/klines")
gold_klines_asset = Asset("clickhouse://gold/fct_daily_klines")

default_args = {
    "owner": "crypto-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_olap_serving",
    default_args=default_args,
    description="Silver Parquet -> ClickHouse -> dbt gold -> BI & lineage exports",
    schedule=[silver_klines_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "olap", "dbt", "bi", "governance"],
) as dag:
    task_load_olap = PythonOperator(
        task_id="load_clickhouse_silver",
        python_callable=load_olap,
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run_gold",
        bash_command="cd /opt/airflow/app/dbt && dbt run",
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test_gold",
        bash_command="cd /opt/airflow/app/dbt && dbt test",
        outlets=[gold_klines_asset],
    )

    task_export_bi = PythonOperator(
        task_id="export_bi_metrics",
        python_callable=export_bi,
    )

    task_export_lineage = PythonOperator(
        task_id="export_lineage_metadata",
        python_callable=export_lineage,
    )

    # Core transformation sequence
    task_load_olap >> task_dbt_run >> task_dbt_test

    # Parallel fan-out after data quality assertions pass
    task_dbt_test >> [task_export_bi, task_export_lineage]
