"""Airflow DAG for batch historical backfill and silver transformation.

Executes parallel Binance historical REST backfill per symbol using
Dynamic Task Mapping, followed by PySpark silver deduplication and partitioning.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

from batch.backfill.binance_rest import backfill_symbol_interval
from batch.config import BatchConfig
from batch.run_silver import main as run_silver
from utils.logging import configure_logging
from utils.storage import make_s3_client

silver_klines_asset = Asset("s3://silver/klines")

default_args = {
    "owner": "crypto-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_backfill_symbol(symbol: str) -> None:
    """Synchronous task callable executing historical backfill for a single symbol.

    Args:
        symbol: Trading-pair symbol (e.g. 'BTCUSDT').
    """
    configure_logging()
    config = BatchConfig()
    s3 = make_s3_client(
        endpoint=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
    )

    async def _runner() -> None:
        for interval in config.kline_intervals:
            await backfill_symbol_interval(
                config=config, s3=s3, symbol=symbol, interval=interval
            )

    asyncio.run(_runner())


with DAG(
    dag_id="crypto_batch_backfill",
    default_args=default_args,
    description="Parallel REST backfill -> PySpark silver lake transformation",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["crypto", "batch", "lakehouse"],
) as dag:
    # Dynamic Task Mapping (.expand): Fanning out symbol backfill in parallel
    task_backfill = PythonOperator.partial(
        task_id="backfill_symbol",
        python_callable=run_backfill_symbol,
    ).expand(op_kwargs=[{"symbol": sym} for sym in ["BTCUSDT", "ETHUSDT"]])

    task_silver_batch = PythonOperator(
        task_id="transform_silver_batch",
        python_callable=run_silver,
        outlets=[silver_klines_asset],
    )

    task_backfill >> task_silver_batch
