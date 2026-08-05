"""Unit tests validating ML feature configuration schema rules."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ml.features.config import FeatureConfig


def test_feature_config_defaults() -> None:
    """Assert default parameters are loaded cleanly."""
    config = FeatureConfig()
    assert config.seq_length == 60
    assert config.target_horizon == 1
    assert "BTCUSDT" in config.symbols


def test_feature_config_invalid_params() -> None:
    """Assert validation flags negative sequences or horizons."""
    with pytest.raises(ValidationError):
        FeatureConfig(seq_length=-5)

    with pytest.raises(ValidationError):
        FeatureConfig(target_horizon=0)
