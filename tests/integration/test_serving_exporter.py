"""Integration tests for the Cube.js REST BI exporter.

Uses ``pytest-httpx`` to mock Cube API responses, verifying JSON transformation
to CSV files and integration flows.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_httpx import HTTPXMock

from olap.config import BiExporterConfig
from olap.exporter import export_semantic_data


@pytest.fixture
def mock_cube_api(httpx_mock: HTTPXMock) -> None:
    """Mock the Cube.js REST load endpoints with standard metric data."""
    # 1. Mock daily klines view response
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:4000/cubejs-api/v1/load",
        json={
            "data": [
                {
                    "ohlcv_daily.symbol": "BTCUSDT",
                    "ohlcv_daily.trade_date": "2024-01-01T00:00:00.000",
                    "ohlcv_daily.total_volume": "100.5",
                    "ohlcv_daily.total_quote_volume": "4221000.0",
                    "ohlcv_daily.total_trades": "1000",
                    "ohlcv_daily.avg_close": "42000.0",
                    "ohlcv_daily.max_high": "42500.0",
                    "ohlcv_daily.min_low": "41500.0",
                }
            ]
        },
    )

    # 2. Mock hourly klines view response
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:4000/cubejs-api/v1/load",
        json={
            "data": [
                {
                    "ohlcv_hourly.symbol": "BTCUSDT",
                    "ohlcv_hourly.hour_at": "2024-01-01T12:00:00.000",
                    "ohlcv_hourly.total_volume": "10.5",
                    "ohlcv_hourly.total_quote_volume": "441000.0",
                    "ohlcv_hourly.total_trades": "100",
                    "ohlcv_hourly.avg_close": "42000.0",
                    "ohlcv_hourly.max_high": "42200.0",
                    "ohlcv_hourly.min_low": "41800.0",
                }
            ]
        },
    )

    # 3. Mock price returns view response
    httpx_mock.add_response(
        method="POST",
        url="http://localhost:4000/cubejs-api/v1/load",
        json={
            "data": [
                {
                    "price_analytics.symbol": "BTCUSDT",
                    "price_analytics.interval": "1m",
                    "price_analytics.open_at": "2024-01-01T12:00:00.000",
                    "price_analytics.avg_log_return": "0.0015",
                    "price_analytics.stddev_log_return": "0.0005",
                }
            ]
        },
    )


def test_exporter_fetches_and_writes_csv(
    tmp_path: Path,
    mock_cube_api: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that export_semantic_data fetches Cube REST views and dumps CSVs."""
    # Prevent gspread authentication attempt if any
    mock_gspread = MagicMock()
    monkeypatch.setattr("gspread.service_account", mock_gspread)

    config = BiExporterConfig(
        _env_file=None,  # type: ignore[call-arg]
        cube_api_url="http://localhost:4000",
        export_output_dir=tmp_path,
        google_service_account_json=None,
    )

    export_semantic_data(config)

    # Verify output files exist
    daily_csv = tmp_path / "fct_ohlcv_daily.csv"
    hourly_csv = tmp_path / "fct_ohlcv_hourly.csv"
    returns_csv = tmp_path / "fct_price_analytics.csv"

    assert daily_csv.exists()
    assert hourly_csv.exists()
    assert returns_csv.exists()

    # Read back daily CSV and assert prefixes are cleaned and values are float
    df_daily = pd.read_csv(daily_csv)
    assert len(df_daily) == 1
    assert "symbol" in df_daily.columns
    assert "total_volume" in df_daily.columns
    assert df_daily.iloc[0]["symbol"] == "BTCUSDT"
    assert float(df_daily.iloc[0]["total_volume"]) == 100.5

    # Read back returns CSV and assert volatility column
    df_returns = pd.read_csv(returns_csv)
    assert len(df_returns) == 1
    assert "stddev_log_return" in df_returns.columns
    assert float(df_returns.iloc[0]["stddev_log_return"]) == 0.0005
