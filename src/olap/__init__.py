"""OLAP loading package — MinIO silver → ClickHouse.

Reads silver-layer Parquet files from MinIO and bulk-inserts them into
ClickHouse using the columnar Arrow insert path for maximum throughput.
"""

from __future__ import annotations

__all__: list[str] = []
