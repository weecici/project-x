"""Unit tests validating the CryptoLSTM neural network architecture."""

from __future__ import annotations

import torch

from ml.training.model import CryptoLSTM


def test_lstm_forward_pass() -> None:
    """Validate shape integrity of the forward pass execution."""
    batch_size = 4
    seq_length = 60
    n_features = 11

    # Create dummy inputs
    inputs = torch.randn(batch_size, seq_length, n_features)

    # Initialize model
    model = CryptoLSTM(input_size=n_features, hidden_size=128, num_layers=2)
    model.eval()

    with torch.no_grad():
        outputs = model(inputs)

    # Output shape should be (batch_size, 2) since it's binary classification
    assert outputs.shape == (batch_size, 2)


def test_lstm_parameter_initialization() -> None:
    """Validate weights are non-zero after custom orthogonal/Xavier init."""
    model = CryptoLSTM(input_size=11, hidden_size=64, num_layers=1)

    # Check that linear classifier has parameters initialized
    assert torch.sum(torch.abs(model.classifier.weight)) > 0.0

    # Check that LSTM weights are populated
    for cell in model.lstm:
        assert torch.sum(torch.abs(cell.weight_hh_l0)) > 0.0
