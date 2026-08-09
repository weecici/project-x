"""Airflow DAG for automated ML model retraining sensor and drift evaluation.

Monitors dbt data quality assertions on gold feature tables, evaluates row
freshness and drift, and triggers downstream PyTorch retraining pipelines.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

from ml.features.run_feature_eng import main as run_feature_eng
from ml.optimization.run_optimize import main as run_optimize
from ml.training.run_train import main as run_train
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
    task_feature_eng = PythonOperator(
        task_id="run_feature_engineering",
        python_callable=run_feature_eng,
        op_kwargs={"args": []},
    )

    task_train = PythonOperator(
        task_id="run_model_training",
        python_callable=run_train,
    )

    task_optimize = PythonOperator(
        task_id="run_model_optimization",
        python_callable=run_optimize,
    )

    task_lineage = PythonOperator(
        task_id="export_lineage_metadata",
        python_callable=export_lineage,
    )

    task_feature_eng >> task_train >> task_optimize >> task_lineage
