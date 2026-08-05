"""Configuration settings for PyTorch model training and tracking in Phase 8."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TrainingConfig(BaseSettings):
    """Configuration class for model training hyperparameters and tracking."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # LSTM Hyperparameters
    hidden_size: int = Field(
        default=256, description="Dimension size of hidden layers."
    )
    num_layers: int = Field(default=3, description="Number of stacked LSTM layers.")
    dropout: float = Field(default=0.3, description="Dropout rate in the LSTM layers.")
    seq_length: int = Field(default=60, description="Input sequence lookback length.")

    # Optimization Hyperparameters
    batch_size: int = Field(default=64, description="Training batch size.")
    epochs: int = Field(default=30, description="Number of training epochs.")
    lr: float = Field(default=1e-3, description="Learning rate for Adam optimizer.")

    # Model metadata
    model_name: str = Field(
        default="crypto_lstm",
        description="Name of the model in the MLflow model registry.",
    )
    device: str = Field(
        default="cuda",
        description="Execution hardware target ('cuda' or 'cpu').",
    )

    # MLflow settings
    mlflow_tracking_uri: str = Field(default="http://localhost:5000")
    mlflow_experiment_name: str = Field(default="crypto_price_direction")

    # StatsD configuration (for emission of metrics to Prometheus/Grafana)
    statsd_host: str = Field(default="localhost")
    statsd_port: int = Field(default=8125)
    statsd_prefix: str = Field(default="ml_training")
