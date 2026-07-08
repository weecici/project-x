"""PySpark silver-layer transformer for kline data.

Reads all bronze kline Parquet files from MinIO, applies the following
transformations, and writes the result to the MinIO silver bucket:

1. **Schema enforcement** — reads with an explicit ``StructType`` schema,
   refusing any implicit type inference.
2. **Type casting** — price/volume string columns → ``DecimalType(18, 8)``;
   millisecond epoch integers → ``TimestampType``.
3. **Deduplication** — on ``(symbol, interval, open_time)``; keeps the
   latest ingested record when live and historical data overlap.
4. **Validation** — rows with null required fields or negative volume are
   filtered out; rejects are written to ``silver/klines_rejected/``.
5. **Partitioned write** — output is Hive-partitioned by
   ``symbol / interval / year / month``; written as Snappy Parquet with
   ``overwrite`` mode per dynamic partition (idempotent re-runs).

PySpark runs in ``local[*]`` mode — no cluster or worker containers.
"""

from __future__ import annotations

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from batch.config import BatchConfig

# Explicit schema for bronze kline Parquet files produced by the backfill
# and live writer. Prices/volumes are stored as strings in bronze to
# preserve Binance decimal precision.
_BRONZE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("interval", StringType(), nullable=False),
        StructField("open_time", LongType(), nullable=False),
        StructField("open", StringType(), nullable=False),
        StructField("high", StringType(), nullable=False),
        StructField("low", StringType(), nullable=False),
        StructField("close", StringType(), nullable=False),
        StructField("volume", StringType(), nullable=False),
        StructField("close_time", LongType(), nullable=False),
        StructField("quote_volume", StringType(), nullable=False),
        StructField("num_trades", LongType(), nullable=False),
        StructField("taker_buy_base_volume", StringType(), nullable=False),
        StructField("taker_buy_quote_volume", StringType(), nullable=False),
    ]
)

_DECIMAL_TYPE = DecimalType(18, 8)


def _build_spark_session(config: BatchConfig) -> SparkSession:
    """Build and return a PySpark session configured for local MinIO access.

    Args:
        config: Resolved ``BatchConfig`` instance.

    Returns:
        A ``SparkSession`` in ``local[*]`` mode with S3A pointing at MinIO.
    """
    return (
        SparkSession.builder.appName("crypto-platform-silver-klines")
        .master("local[*]")
        .config("spark.driver.memory", config.spark_driver_memory)
        .config("spark.executor.memory", config.spark_executor_memory)
        # S3A connector → MinIO
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
        .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        # Reduce default shuffle partitions for local single-node runs.
        .config("spark.sql.shuffle.partitions", "8")
        # Silence noisy Spark/Hadoop INFO logs.
        .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=WARN,console")
        .getOrCreate()
    )


def run_silver(config: BatchConfig) -> None:
    """Execute the bronze → silver transformation for kline data.

    Reads all bronze kline Parquet from MinIO, deduplicates, casts types,
    validates, adds year/month partition columns, and writes to silver.

    Args:
        config: Resolved ``BatchConfig`` instance.
    """
    spark = _build_spark_session(config)
    bronze_path = f"s3a://{config.minio_bucket_bronze}/klines/"
    silver_path = f"s3a://{config.minio_bucket_silver}/klines/"
    rejected_path = f"s3a://{config.minio_bucket_silver}/klines_rejected/"

    logger.info(
        "Silver job started | bronze={bronze} silver={silver}",
        bronze=bronze_path,
        silver=silver_path,
    )

    # ------------------------------------------------------------------
    # 1. Read bronze with explicit schema
    # ------------------------------------------------------------------
    df = spark.read.schema(_BRONZE_SCHEMA).parquet(bronze_path)
    raw_count = df.count()
    logger.info("Read {n} bronze rows", n=raw_count)

    # ------------------------------------------------------------------
    # 2. Cast string price/volume columns to Decimal; ms → Timestamp
    # ------------------------------------------------------------------
    price_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]

    for col in price_cols:
        df = df.withColumn(col, F.col(col).cast(_DECIMAL_TYPE))

    df = df.withColumn(
        "open_ts", (F.col("open_time") / 1000).cast(TimestampType())
    ).withColumn("close_ts", (F.col("close_time") / 1000).cast(TimestampType()))

    # ------------------------------------------------------------------
    # 3. Validate: split valid rows from rejects
    # ------------------------------------------------------------------
    null_check = (
        F.col("symbol").isNull()
        | F.col("open").isNull()
        | F.col("close").isNull()
        | F.col("volume").isNull()
        | (F.col("volume") < 0)
    )
    rejected = df.filter(null_check)
    df = df.filter(~null_check)

    reject_count = rejected.count()
    if reject_count > 0:
        logger.warning(
            "Writing {n} rejected rows to {path}", n=reject_count, path=rejected_path
        )
        rejected.write.mode("append").parquet(rejected_path)

    # ------------------------------------------------------------------
    # 4. Deduplicate on (symbol, interval, open_time) — keep last write
    # ------------------------------------------------------------------
    # Window: partition by dedup key, order by close_time desc so the
    # most-recently-closed bar wins when live + historical overlap.
    from pyspark.sql.window import Window

    dedup_window = Window.partitionBy("symbol", "interval", "open_time").orderBy(
        F.col("close_time").desc()
    )
    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    dedup_count = df.count()
    logger.info(
        "After dedup: {n} rows (removed {d} duplicates)",
        n=dedup_count,
        d=raw_count - reject_count - dedup_count,
    )

    # ------------------------------------------------------------------
    # 5. Add partition columns (year, month) derived from open_time
    # ------------------------------------------------------------------
    df = df.withColumn("year", F.year("open_ts")).withColumn(
        "month", F.month("open_ts")
    )

    # ------------------------------------------------------------------
    # 6. Write silver — partitioned, Snappy, idempotent
    # ------------------------------------------------------------------
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("symbol", "interval", "year", "month")
        .parquet(silver_path)
    )
    logger.info(
        "Silver write complete | rows={n} path={path}",
        n=dedup_count,
        path=silver_path,
    )
    spark.stop()
