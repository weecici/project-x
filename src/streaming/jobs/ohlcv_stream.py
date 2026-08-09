"""PySpark Structured Streaming job to ingest and sink closed kline events.

Consumes raw.klines from Kafka, filters for final closed 1m bars, casts fields
to standard Silver types, and sinks to Delta Lake (MinIO) and Kafka.
"""

from __future__ import annotations

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery  # type: ignore[attr-defined]
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from streaming.config import StreamingConfig
from utils.spark import build_spark_session as common_build_spark_session


def build_spark_session(config: StreamingConfig, app_name: str) -> SparkSession:
    """Build a local Spark session with Kafka, S3A, and Delta Lake connectors.

    Args:
        config: Resolved StreamingConfig parameters.
        app_name: The application name.

    Returns:
        A configured SparkSession instance.
    """
    return common_build_spark_session(
        app_name=app_name,
        driver_memory=config.spark_driver_memory,
        executor_memory=config.spark_executor_memory,
        minio_endpoint=config.minio_endpoint,
        minio_access_key=config.minio_access_key,
        minio_secret_key=config.minio_secret_key,
        shuffle_partitions=4,
        additional_packages=[
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
            "io.delta:delta-spark_4.1_2.13:4.3.1",
        ],
        spark_config={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": (
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            ),
        },
    )


def run_ohlcv_stream(config: StreamingConfig) -> list[StreamingQuery]:
    """Execute the kline streaming pipeline.

    Reads raw.klines from Kafka, extracts closed 1m bars, casts types, and launches
    two concurrent streaming queries writing to both MinIO (Delta Lake) and Kafka.

    Args:
        config: Resolved StreamingConfig parameters.

    Returns:
        List of active StreamingQuery handles.
    """
    spark = build_spark_session(config, "crypto-platform-stream-ohlcv")

    logger.info("Initializing OHLCV streaming pipeline...")

    # Explicit schema mapping Pydantic model names (from WS client dumps)
    kline_schema = StructType(
        [
            StructField("open_time", LongType(), False),
            StructField("close_time", LongType(), False),
            StructField("symbol", StringType(), False),
            StructField("interval", StringType(), False),
            StructField("open", StringType(), False),
            StructField("close", StringType(), False),
            StructField("high", StringType(), False),
            StructField("low", StringType(), False),
            StructField("volume", StringType(), False),
            StructField("number_of_trades", LongType(), False),
            StructField("is_closed", BooleanType(), False),
            StructField("quote_volume", StringType(), False),
        ]
    )

    event_schema = StructType(
        [
            StructField("event_type", StringType(), False),
            StructField("event_time", LongType(), False),
            StructField("symbol", StringType(), False),
            StructField("kline", kline_schema, False),
        ]
    )

    # 1. Read raw stream from Kafka topic
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribePattern", config.kafka_topic_klines)
        .option("startingOffsets", config.kafka_starting_offsets)
        .load()
    )

    # 2. Parse JSON payloads
    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_payload")
        .select(F.from_json(F.col("json_payload"), event_schema).alias("data"))
        .select("data.*")
    )

    # 3. Cast fields to matching Silver layout and extract nested elements
    decimal_type = DecimalType(18, 8)
    flat_df = parsed_df.select(
        F.col("event_time"),
        F.col("symbol"),
        F.col("kline.open_time").alias("open_time"),
        F.col("kline.close_time").alias("close_time"),
        F.col("kline.interval").alias("interval"),
        F.col("kline.open").cast(decimal_type).alias("open"),
        F.col("kline.high").cast(decimal_type).alias("high"),
        F.col("kline.low").cast(decimal_type).alias("low"),
        F.col("kline.close").cast(decimal_type).alias("close"),
        F.col("kline.volume").cast(decimal_type).alias("volume"),
        F.col("kline.number_of_trades").alias("num_trades"),
        F.col("kline.quote_volume").cast(decimal_type).alias("quote_volume"),
        F.col("kline.is_closed").alias("is_closed"),
    )

    # 4. Filter only closed bars (we don't want intermediate aggregations)
    closed_df = flat_df.filter(F.col("is_closed"))

    # Prepare directories and queries
    silver_path = f"s3a://{config.minio_bucket_silver}/klines_stream"
    checkpoint_delta = f"s3a://{config.minio_bucket_silver}/checkpoints/ohlcv_delta"
    checkpoint_kafka = f"s3a://{config.minio_bucket_silver}/checkpoints/ohlcv_kafka"

    logger.info(
        "Sinks configured | Delta Path={path} Kafka Topic={topic}",
        path=silver_path,
        topic=config.kafka_topic_agg_klines,
    )

    # 5. Start Delta Lake Sink query
    delta_query = (
        closed_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_delta)
        .start(silver_path)
    )

    # 6. Prepare and start Kafka JSON Sink query
    kafka_output_df = closed_df.select(
        F.col("symbol").alias("key"),
        F.to_json(F.struct("*")).alias("value"),
    )
    kafka_query = (
        kafka_output_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("topic", config.kafka_topic_agg_klines)
        .option("checkpointLocation", checkpoint_kafka)
        .start()
    )

    return [delta_query, kafka_query]
