"""PySpark Structured Streaming job to ingest trades and compute microstructure metrics.

Consumes raw.trades from Kafka, applies event-time watermarking, filters duplicates,
and aggregates ticks into 1-minute tumbling windows to calculate:
1. VWAP (Volume-Weighted Average Price)
2. Order Flow Imbalance (OFI / net buyer pressure)
3. execution price volatility (rolling standard deviation)
4. trade counts

Outputs are dual-sunk to MinIO (Delta Lake) and Kafka in append mode.
"""

from __future__ import annotations

from loguru import logger
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery  # type: ignore[attr-defined]
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.config import StreamingConfig
from streaming.jobs.ohlcv_stream import build_spark_session


def run_vwap_stream(config: StreamingConfig) -> list[StreamingQuery]:
    """Execute the trade microstructure streaming pipeline.

    Reads raw.trades from Kafka, filters duplicates, aggregates trade executions into
    1-minute event-time tumbling windows with a watermark, and writes results to
    MinIO (Delta Lake) and Kafka.

    Args:
        config: Resolved StreamingConfig parameters.

    Returns:
        List of active StreamingQuery handles.
    """
    spark = build_spark_session(config, "crypto-platform-stream-vwap")

    logger.info("Initializing VWAP and microstructure streaming pipeline...")

    # Explicit schema mapping Pydantic model names (from WS client dumps)
    trade_schema = StructType(
        [
            StructField("event_type", StringType(), False),
            StructField("event_time", LongType(), False),
            StructField("symbol", StringType(), False),
            StructField("trade_id", LongType(), False),
            StructField("price", StringType(), False),
            StructField("quantity", StringType(), False),
            StructField("buyer_order_id", LongType(), True),
            StructField("seller_order_id", LongType(), True),
            StructField("trade_time", LongType(), False),
            StructField("is_buyer_maker", BooleanType(), False),
        ]
    )

    # 1. Read raw trades stream from Kafka topic
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribePattern", config.kafka_topic_trades)
        .option("startingOffsets", config.kafka_starting_offsets)
        .load()
    )

    # 2. Parse JSON payloads
    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_payload")
        .select(F.from_json(F.col("json_payload"), trade_schema).alias("data"))
        .select("data.*")
    )

    # 3. Cast values to DoubleType for mathematical computations
    typed_df = parsed_df.select(
        F.col("symbol"),
        F.col("trade_id"),
        F.col("price").cast(DoubleType()).alias("price"),
        F.col("quantity").cast(DoubleType()).alias("quantity"),
        F.col("trade_time"),
        F.col("is_buyer_maker"),
        # Parse timestamp from epoch milliseconds for event-time watermarking
        (F.col("trade_time") / 1000).cast(TimestampType()).alias("trade_time_ts"),
    )

    # 4. Set event-time watermark to handle late-arriving trade events
    watermarked_df = typed_df.withWatermark(
        "trade_time_ts", f"{config.stream_watermark_delay_seconds} seconds"
    )

    # 5. Drop duplicates within the watermark window using trade_id
    deduplicated_df = watermarked_df.dropDuplicatesWithinWatermark(["trade_id"])

    # 6. Apply tumbling window aggregation (duration defined in configuration)
    window_duration = f"{config.stream_window_duration_minutes} minutes"
    agg_df = deduplicated_df.groupBy(
        F.col("symbol"), F.window(F.col("trade_time_ts"), window_duration)
    ).agg(
        # VWAP = sum(price * qty) / sum(qty)
        (F.sum(F.col("price") * F.col("quantity")) / F.sum(F.col("quantity"))).alias(
            "vwap"
        ),
        # OFI = sum(qty) for Taker Buys - sum(qty) for Maker Buys
        F.sum(
            F.when(~F.col("is_buyer_maker"), F.col("quantity")).otherwise(
                -F.col("quantity")
            )
        ).alias("order_flow_imbalance"),
        # Volatility = stddev of execution prices, coalesce to 0.0
        F.coalesce(F.stddev_samp(F.col("price")), F.lit(0.0)).alias("price_volatility"),
        # Total Trades in window
        F.count("*").alias("trade_count"),
    )

    # 7. Flatten window struct into separate event columns
    flat_agg_df = agg_df.select(
        F.col("symbol"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("vwap"),
        F.col("order_flow_imbalance"),
        F.col("price_volatility"),
        F.col("trade_count"),
    )

    # Prepare directories and queries
    silver_path = f"s3a://{config.minio_bucket_silver}/vwap_stream"
    checkpoint_delta = f"s3a://{config.minio_bucket_silver}/checkpoints/vwap_delta"
    checkpoint_kafka = f"s3a://{config.minio_bucket_silver}/checkpoints/vwap_kafka"

    logger.info(
        "Sinks configured | Delta Path={path} Kafka Topic={topic}",
        path=silver_path,
        topic=config.kafka_topic_agg_vwap,
    )

    # 8. Start Delta Lake Sink query
    delta_query = (
        flat_agg_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_delta)
        .start(silver_path)
    )

    # 9. Prepare and start Kafka JSON Sink query (requires key/value strings)
    kafka_output_df = flat_agg_df.select(
        F.col("symbol").alias("key"),
        # Convert window start/end timestamps to strings for JSON serialisation
        F.to_json(
            F.struct(
                F.col("symbol"),
                F.col("window_start").cast(StringType()).alias("window_start"),
                F.col("window_end").cast(StringType()).alias("window_end"),
                F.col("vwap"),
                F.col("order_flow_imbalance"),
                F.col("price_volatility"),
                F.col("trade_count"),
            )
        ).alias("value"),
    )
    kafka_query = (
        kafka_output_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("topic", config.kafka_topic_agg_vwap)
        .option("checkpointLocation", checkpoint_kafka)
        .start()
    )

    return [delta_query, kafka_query]
