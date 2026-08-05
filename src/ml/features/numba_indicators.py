"""Numba-accelerated time-series indicators for high-performance quantitative

feature engineering. Contains JIT-compiled versions of EMA, MACD, and RSI, with
benchmarking functions to compare performance against pure Python implementations.
"""

from __future__ import annotations

import time
import typing

import mlflow
import numpy as np
import pandas as pd
import pandas_ta as ta
from numba import njit


@njit(cache=True)  # type: ignore[untyped-decorator]
def numba_ema(
    values: np.ndarray[typing.Any, typing.Any], period: int
) -> np.ndarray[typing.Any, typing.Any]:
    """Calculate the Exponential Moving Average (EMA).

    Args:
        values: 1D float array of prices/values.
        period: Lookback window period.

    Returns:
        EMA array of same shape.
    """
    n = len(values)
    ema = np.empty(n, dtype=np.float64)
    if n == 0:
        return ema
    alpha = 2.0 / (period + 1)
    ema[0] = values[0]
    for i in range(1, n):
        ema[i] = values[i] * alpha + ema[i - 1] * (1.0 - alpha)
    return ema


@njit(cache=True)  # type: ignore[untyped-decorator]
def numba_rsi(
    prices: np.ndarray[typing.Any, typing.Any], period: int = 14
) -> np.ndarray[typing.Any, typing.Any]:
    """Calculate the Relative Strength Index (RSI).

    Args:
        prices: 1D float array of prices.
        period: Lookback window period (default 14).

    Returns:
        RSI array of same shape.
    """
    n = len(prices)
    rsi = np.empty(n, dtype=np.float64)
    rsi[:period] = 50.0  # Default midpoint
    if n <= period:
        return rsi

    # Calculate gains and losses
    gains = np.zeros(n)
    losses = np.zeros(n)
    for i in range(1, n):
        diff = prices[i] - prices[i - 1]
        if diff > 0:
            gains[i] = diff
        else:
            losses[i] = -diff

    # Initial average gains/losses
    avg_gain = 0.0
    avg_loss = 0.0
    for i in range(1, period + 1):
        avg_gain += gains[i]
        avg_loss += losses[i]
    avg_gain /= period
    avg_loss /= period

    alpha = 1.0 / period
    if avg_loss == 0.0:
        rsi[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi[period] = 100.0 - (100.0 / (1.0 + rs))

    for i in range(period + 1, n):
        avg_gain = gains[i] * alpha + avg_gain * (1.0 - alpha)
        avg_loss = losses[i] * alpha + avg_loss * (1.0 - alpha)
        if avg_loss == 0.0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))

    return rsi


@njit(cache=True)  # type: ignore[untyped-decorator]
def numba_macd(
    prices: np.ndarray[typing.Any, typing.Any],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> tuple[
    np.ndarray[typing.Any, typing.Any],
    np.ndarray[typing.Any, typing.Any],
    np.ndarray[typing.Any, typing.Any],
]:
    """Calculate MACD (Moving Average Convergence Divergence) lines.

    Args:
        prices: 1D float array of prices.
        fast_period: Short period.
        slow_period: Long period.
        signal_period: Signal smoothing period.

    Returns:
        Tuple of (macd_line, signal_line, macd_histogram).
    """
    fast_ema = numba_ema(prices, fast_period)
    slow_ema = numba_ema(prices, slow_period)
    macd_line = fast_ema - slow_ema
    signal_line = numba_ema(macd_line, signal_period)
    macd_hist = macd_line - signal_line
    return macd_line, signal_line, macd_hist


def python_rsi_baseline(
    prices: np.ndarray[typing.Any, typing.Any], period: int = 14
) -> np.ndarray[typing.Any, typing.Any]:
    """Pure Python, non-JIT implementation of RSI for benchmark baseline."""
    n = len(prices)
    rsi = np.empty(n, dtype=np.float64)
    rsi[:period] = 50.0
    if n <= period:
        return rsi

    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        diff = prices[i] - prices[i - 1]
        if diff > 0:
            gains[i] = diff
        else:
            losses[i] = -diff

    avg_gain = sum(gains[1 : period + 1]) / period
    avg_loss = sum(losses[1 : period + 1]) / period

    alpha = 1.0 / period
    if avg_loss == 0.0:
        rsi[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi[period] = 100.0 - (100.0 / (1.0 + rs))

    for i in range(period + 1, n):
        avg_gain = gains[i] * alpha + avg_gain * (1.0 - alpha)
        avg_loss = losses[i] * alpha + avg_loss * (1.0 - alpha)
        if avg_loss == 0.0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))

    return rsi


def run_numba_benchmark(n_points: int = 100_000) -> dict[str, float]:
    """Run performance benchmark comparing JIT-compiled Numba vs pure Python.

    Args:
        n_points: Number of data points to generate.

    Returns:
        Dict containing timing results.
    """
    print(f"Generating benchmark dataset with {n_points:,} prices...")
    np.random.seed(42)
    prices = np.cumsum(np.random.randn(n_points)) + 100.0

    # 1. Warm-up JIT
    _ = numba_rsi(prices[:100], 14)

    # 2. Benchmark Numba RSI
    t0 = time.perf_counter()
    rsi_numba = numba_rsi(prices, 14)
    t_numba = time.perf_counter() - t0

    # 3. Benchmark Pure Python RSI
    t0 = time.perf_counter()
    rsi_python = python_rsi_baseline(prices, 14)
    t_python = time.perf_counter() - t0

    # 4. Benchmark pandas-ta RSI
    df = pd.DataFrame({"close": prices})
    t0 = time.perf_counter()
    ta.rsi(df["close"], length=14)
    t_pandas_ta = time.perf_counter() - t0

    # Sanity checks
    assert np.allclose(rsi_numba[20:], rsi_python[20:], equal_nan=True), (
        "Numba and Python implementations must match"
    )

    speedup_python = t_python / t_numba
    speedup_pandas = t_pandas_ta / t_numba

    results = {
        "time_numba_sec": t_numba,
        "time_python_sec": t_python,
        "time_pandas_ta_sec": t_pandas_ta,
        "speedup_vs_pure_python": speedup_python,
        "speedup_vs_pandas_ta": speedup_pandas,
    }

    print("--- Benchmark Results ---")
    print(f"Numba RSI:      {t_numba:.5f}s")
    print(f"Pure Python:    {t_python:.5f}s (Speedup: {speedup_python:.1f}x)")
    print(f"pandas-ta:      {t_pandas_ta:.5f}s (Speedup: {speedup_pandas:.1f}x)")

    # Log to MLflow if active run exists
    if mlflow.active_run():
        mlflow.log_metrics(results)

    return results
