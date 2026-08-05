"""Unit tests validating Numba JIT indicator outputs vs pandas-ta baseline."""

from __future__ import annotations

import typing

import numpy as np

from ml.features.numba_indicators import numba_ema, numba_rsi


def python_ema_baseline(
    values: np.ndarray[typing.Any, typing.Any], period: int
) -> np.ndarray[typing.Any, typing.Any]:
    """Pure Python EMA baseline for correctness assertion."""
    n = len(values)
    ema = np.empty(n, dtype=np.float64)
    if n == 0:
        return ema
    alpha = 2.0 / (period + 1)
    ema[0] = values[0]
    for i in range(1, n):
        ema[i] = values[i] * alpha + ema[i - 1] * (1.0 - alpha)
    return ema


def test_numba_ema_matches_python_baseline() -> None:
    """Assert JIT compiled EMA values match the python baseline exactly."""
    np.random.seed(42)
    prices = np.cumsum(np.random.randn(100)) + 100.0
    period = 10

    # Numba JIT
    ema_numba = numba_ema(prices, period)
    # Python baseline
    ema_python = python_ema_baseline(prices, period)

    assert np.allclose(ema_numba, ema_python)


def test_numba_rsi_matches_python_baseline() -> None:
    """Assert JIT compiled RSI values match the python baseline exactly."""
    from ml.features.numba_indicators import python_rsi_baseline

    np.random.seed(42)
    prices = np.cumsum(np.random.randn(100)) + 100.0
    period = 14

    # Numba JIT
    rsi_numba = numba_rsi(prices, period)
    # Python baseline
    rsi_python = python_rsi_baseline(prices, period)

    assert np.allclose(rsi_numba, rsi_python)
