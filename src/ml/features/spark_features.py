"""PySpark silver-to-gold feature engineering pipeline.

Reads clean silver kline data from MinIO, calculates technical indicators
using native Spark window functions and vectorized Pandas UDFs (pandas-ta),
computes classification targets, scales features, and writes the final gold
features back to MinIO.
"""

from __future__ import annotations

import mlflow
import numpy as np
import pandas as pd
import pandas_ta as ta
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ml.features.config import FeatureConfig
from utils.spark import build_spark_session as common_build_spark_session


def build_spark_session(config: FeatureConfig) -> SparkSession:
    """Build and return a PySpark session configured for MinIO access."""
    return common_build_spark_session(
        app_name="crypto-platform-feature-eng",
        master=config.spark_master,
        driver_memory="2g",
        executor_memory="2g",
        minio_endpoint=config.minio_endpoint,
        minio_access_key=config.minio_access_key,
        minio_secret_key=config.minio_secret_key,
        shuffle_partitions=4,
    )


def compute_features(spark: SparkSession, config: FeatureConfig) -> None:
    """Read silver data, calculate features, scale them, and write gold features."""
    silver_path = f"s3a://{config.minio_bucket_silver}/klines/"
    gold_path = f"s3a://{config.minio_bucket_gold}/ml_features/"

    logger.info("Reading silver kline data from: {}", silver_path)
    # Read the silver klines (Snappy Parquet)
    df = spark.read.parquet(silver_path)

    # Cast close price to float for window operations
    df = df.withColumn("close_float", F.col("close").cast("double"))

    # Define Window specifications
    window_spec_20 = (
        Window.partitionBy("symbol").orderBy("open_time").rowsBetween(-19, 0)
    )
    window_spec_50 = (
        Window.partitionBy("symbol").orderBy("open_time").rowsBetween(-49, 0)
    )

    # Calculate native Spark features
    logger.info(
        "Calculating native Spark window indicators (SMA-20, SMA-50, Volatility-20)"
    )
    df = df.withColumn("sma_20", F.avg("close_float").over(window_spec_20))
    df = df.withColumn("sma_50", F.avg("close_float").over(window_spec_50))
    df = df.withColumn("vol_20", F.stddev("close_float").over(window_spec_20))

    # Fill initial standard deviation nulls with 0
    df = df.fillna({"vol_20": 0.0})

    # Prepare schema for Pandas UDF grouped map
    output_schema = (
        df.select(
            "open_time",
            "symbol",
            "close_float",
            "volume",
            "sma_20",
            "sma_50",
            "vol_20",
        )
        .schema.add("rsi_14", "double")
        .add("macd", "double")
        .add("macd_signal", "double")
        .add("bb_upper", "double")
        .add("bb_lower", "double")
        .add("returns_1", "double")
        .add("target_direction", "long")
    )

    def calculate_pandas_features(pdf: pd.DataFrame) -> pd.DataFrame:
        """Vectorized computation of RSI, MACD, Bollinger Bands, and Target."""
        # Sort in chronological order per symbol group
        pdf = pdf.sort_values("open_time").reset_index(drop=True)
        close_series = pdf["close_float"]

        # RSI-14
        pdf["rsi_14"] = ta.rsi(close_series, length=14)

        # MACD (12, 26, 9)
        macd_df = ta.macd(close_series, fast=12, slow=26, signal=9)
        if macd_df is not None and not macd_df.empty:
            pdf["macd"] = macd_df["MACD_12_26_9"]
            pdf["macd_signal"] = macd_df["MACDs_12_26_9"]
        else:
            pdf["macd"] = np.nan
            pdf["macd_signal"] = np.nan

        # Bollinger Bands-20
        bb_df = ta.bbands(close_series, length=20, std=2.0)  # type: ignore[arg-type]
        if bb_df is not None and not bb_df.empty:
            pdf["bb_upper"] = bb_df["BBU_20_2.0_2.0"]
            pdf["bb_lower"] = bb_df["BBL_20_2.0_2.0"]
        else:
            pdf["bb_upper"] = np.nan
            pdf["bb_lower"] = np.nan

        # Shift target and log returns
        pdf["returns_1"] = np.log(close_series / close_series.shift(1))
        # 1 if t+1 close > t close, else 0
        pdf["target_direction"] = (close_series.shift(-1) > close_series).astype(int)

        # Fill NaNs from lag/lead window initialization
        pdf = pdf.bfill().ffill()
        pdf["returns_1"] = pdf["returns_1"].fillna(0.0)
        pdf["rsi_14"] = pdf["rsi_14"].fillna(50.0)
        pdf["macd"] = pdf["macd"].fillna(0.0)
        pdf["macd_signal"] = pdf["macd_signal"].fillna(0.0)
        pdf["bb_upper"] = pdf["bb_upper"].fillna(pdf["close_float"])
        pdf["bb_lower"] = pdf["bb_lower"].fillna(pdf["close_float"])

        cols_to_return = [
            "open_time",
            "symbol",
            "close_float",
            "volume",
            "sma_20",
            "sma_50",
            "vol_20",
            "rsi_14",
            "macd",
            "macd_signal",
            "bb_upper",
            "bb_lower",
            "returns_1",
            "target_direction",
        ]
        return pdf[cols_to_return]

    logger.info("Running vectorized Pandas UDF for complex indicators")
    features_df = df.groupby("symbol").applyInPandas(
        calculate_pandas_features, schema=output_schema
    )

    # Perform standard scaling on key features
    logger.info("Computing standard scaling metrics for feature sets")

    pd_sample = (
        features_df.select(
            "sma_20",
            "sma_50",
            "vol_20",
            "rsi_14",
            "macd",
            "macd_signal",
            "bb_upper",
            "bb_lower",
            "returns_1",
        )
        .limit(50_000)
        .toPandas()
    )

    pd_sample = pd.DataFrame(pd_sample)

    means = pd_sample.mean().to_dict()
    stds = pd_sample.std().to_dict()

    # Log scaling metrics to MLflow for consistent inference scaling
    if mlflow.active_run():
        for key in means:
            mlflow.log_param(f"scale_mean_{key}", means[key])
            mlflow.log_param(f"scale_std_{key}", stds[key])
        logger.info("Feature scaling parameters successfully logged to MLflow.")

    # Write output Parquet to the gold bucket
    logger.info("Writing final gold features to MinIO: {}", gold_path)
    features_df.write.partitionBy("symbol").mode("overwrite").parquet(gold_path)
    logger.info("Feature engineering run successfully completed.")
