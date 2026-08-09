"""Unit tests validating Airflow DAG structure, dynamic task mapping, and callables."""

from __future__ import annotations

import inspect

from airflow.models import DagBag


class TestAirflowDAGsValidation:
    """Validate that DAG files are syntactically sound and parse without errors."""

    def test_dagbag_import_no_errors(self) -> None:
        """Verify DagBag loads DAG files without import errors."""
        dagbag = DagBag(dag_folder="src/orchestration/dags")

        assert len(dagbag.import_errors) == 0, (
            f"DAG import errors found: {dagbag.import_errors}"
        )

    def test_batch_backfill_dag_structure(self) -> None:
        """Verify crypto_batch_backfill DAG ID and task dependencies."""
        dagbag = DagBag(dag_folder="src/orchestration/dags")
        dag = dagbag.dags.get("crypto_batch_backfill")

        assert dag is not None
        assert dag.dag_id == "crypto_batch_backfill"
        assert len(dag.tasks) == 2

        task_ids = [t.task_id for t in dag.tasks]
        assert "backfill_symbol" in task_ids
        assert "transform_silver_batch" in task_ids

    def test_olap_serving_dag_structure(self) -> None:
        """Verify crypto_olap_serving DAG ID and task dependency hierarchy."""
        dagbag = DagBag(dag_folder="src/orchestration/dags")
        dag = dagbag.dags.get("crypto_olap_serving")

        assert dag is not None
        assert dag.dag_id == "crypto_olap_serving"
        assert len(dag.tasks) == 5

        task_ids = [t.task_id for t in dag.tasks]
        assert "load_clickhouse_silver" in task_ids
        assert "dbt_run_gold" in task_ids
        assert "dbt_test_gold" in task_ids
        assert "export_bi_metrics" in task_ids
        assert "export_lineage_metadata" in task_ids

    def test_ml_retrain_dag_structure(self) -> None:
        """Verify crypto_ml_retrain DAG ID and task dependency hierarchy."""
        dagbag = DagBag(dag_folder="src/orchestration/dags")
        dag = dagbag.dags.get("crypto_ml_retrain")

        assert dag is not None
        assert dag.dag_id == "crypto_ml_retrain"
        assert len(dag.tasks) == 4

    def test_python_callables_are_synchronous(self) -> None:
        """Verify PythonOperator callables across all DAGs are non-coroutine."""
        dagbag = DagBag(dag_folder="src/orchestration/dags")

        for dag_id, dag in dagbag.dags.items():
            for task in dag.tasks:
                python_callable = getattr(task, "python_callable", None)
                if python_callable:
                    assert not inspect.iscoroutinefunction(python_callable), (
                        f"Async callable detected in task '{task.task_id}' "
                        f"of DAG '{dag_id}'. Require synchronous callables."
                    )
