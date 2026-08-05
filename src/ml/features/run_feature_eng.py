"""CLI entrypoint for Phase 8 feature engineering.

Orchestrates the PySpark feature generation pipeline and executes the JIT
Numba performance benchmark, logging results and configuration to MLflow.
"""

from __future__ import annotations

import argparse
import sys

import mlflow
from loguru import logger

from ml.features.config import FeatureConfig
from ml.features.numba_indicators import run_numba_benchmark
from ml.features.spark_features import build_spark_session, compute_features


@mlflow.trace
def main() -> None:
    """Run the feature engineering pipeline and indicator benchmarks."""
    parser = argparse.ArgumentParser(description="Run Phase 8 Feature Engineering")
    parser.add_argument(
        "--n_benchmark_points",
        type=int,
        default=100_000,
        help="Number of data points for Numba indicator benchmark.",
    )
    args = parser.parse_args()

    config = FeatureConfig()

    logger.info("Initializing MLflow client...")
    mlflow.set_tracking_uri(config.mlflow_tracking_uri)
    mlflow.set_experiment(config.mlflow_experiment_name)

    # Use MLflow nested/autologging context if training starts, or simple run here
    logger.info("Starting MLflow run for feature engineering...")
    with mlflow.start_run(run_name="feature_engineering"):
        # Log feature parameters
        mlflow.log_params(
            {
                "symbols": str(config.symbols),
                "seq_length": config.seq_length,
                "target_horizon": config.target_horizon,
            }
        )

        logger.info("Starting PySpark Session...")
        spark = build_spark_session(config)

        try:
            logger.info("Executing Spark feature calculations...")
            compute_features(spark, config)
        except Exception as e:
            logger.error("Spark features execution failed: {}", str(e))
            spark.stop()
            sys.exit(1)

        logger.info("Stopping PySpark Session...")
        spark.stop()

        logger.info("Running JIT Numba Indicators Benchmark...")
        run_numba_benchmark(args.n_benchmark_points)

    logger.info("Feature engineering workflow run successfully completed.")


if __name__ == "__main__":
    main()
