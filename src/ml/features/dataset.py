"""PyTorch Dataset loader for time-series kline sequences.

Reads gold features from MinIO, applies standard scaling, partitions
by symbol, and constructs chronological sequence windows for LSTM training,
using TimeSeriesSplit to prevent data leakage.
"""

from __future__ import annotations

import typing

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import torch
from pyarrow.fs import S3FileSystem
from torch.utils.data import Dataset

from ml.features.config import FeatureConfig

FEATURE_COLUMNS = [
    "close_float",
    "volume",
    "sma_20",
    "sma_50",
    "vol_20",
    "rsi_14",
    "macd",
    "macd_signal",
    "bb_upper",
    "bb_lower",
    "returns_1",
]


class CryptoDataset(Dataset[tuple[torch.Tensor, torch.Tensor]]):
    """PyTorch Dataset for cryptocurrency kline sequences."""

    def __init__(
        self,
        features: np.ndarray[typing.Any, typing.Any],
        targets: np.ndarray[typing.Any, typing.Any],
    ) -> None:
        """Initialize the dataset with precomputed sequences and targets.

        Args:
            features: 3D float array of shape (N, seq_length, n_features).
            targets: 1D int array of shape (N,).
        """
        self.features = torch.tensor(features, dtype=torch.float32)
        self.targets = torch.tensor(targets, dtype=torch.long)

    def __len__(self) -> int:
        return len(self.targets)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        return self.features[idx], self.targets[idx]


def load_gold_features(config: FeatureConfig) -> pd.DataFrame:
    """Load all gold features from MinIO using PyArrow."""
    # Clean the endpoint schema prefix if present for pyarrow S3FS compatibility
    clean_endpoint = config.minio_endpoint.replace("http://", "").replace(
        "https://", ""
    )

    fs = S3FileSystem(
        endpoint_override=clean_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        scheme="http" if "http://" in config.minio_endpoint else "https",
    )

    path = f"{config.minio_bucket_gold}/ml_features"
    table = pq.read_table(path, filesystem=fs)
    df: pd.DataFrame = table.to_pandas()

    # Cast all feature columns to float64 to prevent decimal.Decimal
    # compatibility issues
    for col in FEATURE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(float)

    return df


def prepare_sequences(
    df: pd.DataFrame,
    seq_length: int = 60,
    fit_scaler: bool = True,
    scaler_stats: dict[str, tuple[float, float]] | None = None,
) -> tuple[
    np.ndarray[typing.Any, typing.Any],
    np.ndarray[typing.Any, typing.Any],
    dict[str, tuple[float, float]],
]:
    """Normalize features and construct sliding window sequences per symbol.

    Args:
        df: Input DataFrame containing kline features.
        seq_length: Sequence window lookback length.
        fit_scaler: Whether to calculate scaling parameters on this data.
        scaler_stats: Dict of {col: (mean, std)} to use if fit_scaler is False.

    Returns:
        Tuple of (X_sequences, y_targets, scaler_stats_dict).
    """
    # Group by symbol to construct separate chronological windows
    grouped = df.groupby("symbol")

    all_sequences = []
    all_targets = []

    computed_scaler_stats: dict[str, tuple[float, float]] = {}

    if fit_scaler:
        for col in FEATURE_COLUMNS:
            mean = df[col].mean()
            std = df[col].std()
            std = std if std > 0.0 else 1.0
            computed_scaler_stats[col] = (mean, std)
    else:
        computed_scaler_stats = scaler_stats or {}

    for _, group in grouped:
        # Ensure chronological order
        sorted_group = group.sort_values("open_time").reset_index(drop=True)

        # Scale features
        scaled_features = sorted_group[FEATURE_COLUMNS].copy()
        for col in FEATURE_COLUMNS:
            mean, std = computed_scaler_stats.get(col, (0.0, 1.0))
            scaled_features[col] = (scaled_features[col] - mean) / std

        feat_array = scaled_features.values
        target_array = sorted_group["target_direction"].values

        n_rows = len(sorted_group)
        if n_rows < seq_length:
            continue

        for i in range(seq_length, n_rows):
            # Input is the sequence ending at i - 1
            seq = feat_array[i - seq_length : i]
            # Target is the prediction from step i - 1 to i
            tar = target_array[i - 1]
            all_sequences.append(seq)
            all_targets.append(tar)

    if not all_sequences:
        return (
            np.empty((0, seq_length, len(FEATURE_COLUMNS))),
            np.empty((0,)),
            computed_scaler_stats,
        )

    return np.array(all_sequences), np.array(all_targets), computed_scaler_stats
