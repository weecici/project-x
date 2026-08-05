"""Unit tests validating CryptoDataset sequence generation and scaling."""

from __future__ import annotations

import numpy as np
import pandas as pd

from ml.features.dataset import FEATURE_COLUMNS, CryptoDataset, prepare_sequences


def test_prepare_sequences_shape() -> None:
    """Validate sequence window shape generation on synthetic inputs."""
    seq_length = 10
    n_points = 50

    import typing

    # Generate synthetic DataFrame for a symbol
    df_data = []
    for i in range(n_points):
        row: dict[str, typing.Any] = {col: float(i + 1) for col in FEATURE_COLUMNS}
        row["open_time"] = int(1000 + i * 60)
        row["symbol"] = "BTCUSDT"
        row["target_direction"] = int(i % 2)
        df_data.append(row)

    df = pd.DataFrame(df_data)

    X, y, stats = prepare_sequences(df, seq_length=seq_length, fit_scaler=True)

    # N expected samples = n_points - seq_length = 40
    assert len(y) == n_points - seq_length
    assert X.shape == (n_points - seq_length, seq_length, len(FEATURE_COLUMNS))
    assert len(stats) == len(FEATURE_COLUMNS)

    # Assert mean is mapped in stats
    assert "close_float" in stats
    mean, _std = stats["close_float"]
    assert np.isclose(mean, df["close_float"].mean())


def test_crypto_dataset_indexing() -> None:
    """Validate torch dataset wraps features and targets correctly."""
    X = np.random.randn(5, 10, 11)
    y = np.array([0, 1, 0, 1, 0])

    dataset = CryptoDataset(X, y)
    assert len(dataset) == 5

    x_tensor, y_tensor = dataset[2]
    assert x_tensor.shape == (10, 11)
    assert y_tensor.item() == 0
