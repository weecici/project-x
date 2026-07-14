"""Real-time stream processing package using PySpark Structured Streaming.

This package ingests trades and klines from Kafka, performs event-time windowed
aggregations (OHLCV, VWAP, Order Flow Imbalance, Volatility), and writes the outputs
to low-latency Kafka topics and durable Delta Lake tables on MinIO.
"""

from __future__ import annotations
