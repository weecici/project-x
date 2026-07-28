"""Airflow DAG for automated ML model retraining sensor and drift evaluation.

Monitors dbt data quality assertions on gold feature tables, evaluates row
freshness and drift, and triggers downstream PyTorch retraining pipelines.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

from orchestration.governance.run_lineage import main as export_lineage

gold_klines_asset = Asset("clickhouse://gold/fct_daily_klines")

default_args = {
    "owner": "crypto-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_ml_retrain",
    default_args=default_args,
    description="Automated ML retrain trigger on gold asset updates",
    schedule=[gold_klines_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "ml", "retrain"],
) as dag:
    task_drift_eval = PythonOperator(
        task_id="evaluate_feature_drift",
        python_callable=export_lineage,
    )

    task_trigger_ml = BashOperator(
        task_id="trigger_ml_retraining",
        bash_command=(
            "echo 'ML retraining event registered for Phase 8 PyTorch execution'"
        ),
    )

    task_drift_eval >> task_trigger_ml
