"""Crypto-platform batch processing package.

Provides historical data backfill via the Binance REST API and PySpark
silver-layer transformations (deduplication, schema enforcement, and
Hive-partitioned Parquet output).
"""

from __future__ import annotations

__all__: list[str] = []
