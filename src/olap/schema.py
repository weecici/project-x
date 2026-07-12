"""Database schema DDL definitions for ClickHouse."""

from __future__ import annotations

# DDL for raw kline table inside the silver database.
# Engine: ReplacingMergeTree(_loaded_at)
#   Deduplicates by PRIMARY KEY (symbol, interval, open_time) on background merge,
#   keeping the row with the highest _loaded_at. Idempotent re-loads are safe.
# Partitioning: (symbol, toYYYYMM(open_time))
#   Keeps partition files small; enables partition-level pruning in gold models.
KLINES_RAW_DDL = """
CREATE TABLE IF NOT EXISTS silver.klines_raw
(
    symbol                  LowCardinality(String),
    interval                LowCardinality(String),
    open_time               DateTime64(3, 'UTC'),
    open                    Decimal(18, 8),
    high                    Decimal(18, 8),
    low                     Decimal(18, 8),
    close                   Decimal(18, 8),
    volume                  Decimal(18, 8),
    close_time              DateTime64(3, 'UTC'),
    quote_volume            Decimal(18, 8),
    num_trades              UInt32,
    taker_buy_base_volume   Decimal(18, 8),
    taker_buy_quote_volume  Decimal(18, 8),
    _loaded_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY (symbol, toYYYYMM(open_time))
ORDER BY (symbol, interval, open_time)
SETTINGS index_granularity = 8192
"""
