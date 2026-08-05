"""CLI entrypoint for Phase 8 sequence model training.

Loads feature datasets from MinIO, splits them chronologically, sets up
reproducible MLflow experiment tracking, and executes the PyTorch training loop.
"""

from __future__ import annotations

import hashlib
import os
import subprocess

import mlflow
from loguru import logger
from torch.utils.data import DataLoader

from ml.features.config import FeatureConfig
from ml.features.dataset import CryptoDataset, load_gold_features, prepare_sequences
from ml.training.config import TrainingConfig
from ml.training.model import CryptoLSTM
from ml.training.trainer import train_model


def get_git_revision_hash() -> str:
    """Get the current git commit revision hash for training lineage."""
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"])
            .decode("ascii")
            .strip()
        )
    except Exception:
        return "unknown"


@mlflow.trace
def main() -> None:
    """Execute the model training pipeline."""
    # Load configuration structures
    train_config = TrainingConfig()
    feat_config = FeatureConfig()

    # Export MinIO variables to environment for boto3/MLflow client authentication
    os.environ["AWS_ACCESS_KEY_ID"] = feat_config.minio_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = feat_config.minio_secret_key
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = feat_config.minio_endpoint
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    logger.info("Initializing MLflow tracking configuration...")
    mlflow.set_tracking_uri(train_config.mlflow_tracking_uri)
    mlflow.set_experiment(train_config.mlflow_experiment_name)

    # Enable autologging for PyTorch
    mlflow.pytorch.autolog(log_every_n_epoch=1, log_models=False)

    # Enable MLflow system metrics logging with a 1-second sampling interval
    mlflow.enable_system_metrics_logging()  # type: ignore[no-untyped-call]
    mlflow.set_system_metrics_sampling_interval(1)  # type: ignore[no-untyped-call]

    logger.info("Loading gold features from MinIO storage...")
    try:
        df_features = load_gold_features(feat_config)
    except Exception as e:
        logger.error("Failed to load gold features from MinIO: {}", e)
        return

    if df_features.empty:
        logger.error("Loaded feature dataset is empty. Cannot proceed.")
        return

    # Calculate dataset fingerprint for version control lineage
    data_bytes = hashlib.md5(df_features.to_string().encode("utf-8")).hexdigest()

    logger.info("Splitting features and targets chronologically...")
    X, y, _scaler_stats = prepare_sequences(
        df=df_features,
        seq_length=train_config.seq_length,
        fit_scaler=True,
    )

    n_samples = len(y)
    if n_samples == 0:
        logger.error("No valid sequences generated. Lookback sequence length too long?")
        return

    logger.info("Total generated sequence samples: {}", n_samples)

    # 80/20 train/validation temporal split (no shuffling to prevent lookahead leakage)
    split_idx = int(n_samples * 0.8)
    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]

    logger.info("Train samples: {}, Val samples: {}", len(y_train), len(y_val))

    train_dataset = CryptoDataset(X_train, y_train)
    val_dataset = CryptoDataset(X_val, y_val)

    train_loader = DataLoader(
        train_dataset,
        batch_size=train_config.batch_size,
        shuffle=True,
        drop_last=True,
    )
    val_loader = DataLoader(
        val_dataset,
        batch_size=train_config.batch_size,
        shuffle=False,
    )

    # Instantiate PyTorch LSTM module
    input_size = X_train.shape[2]
    model = CryptoLSTM(
        input_size=input_size,
        hidden_size=train_config.hidden_size,
        num_layers=train_config.num_layers,
        dropout=train_config.dropout,
    )

    git_hash = get_git_revision_hash()

    logger.info("Starting MLflow training run...")
    with mlflow.start_run(run_name="train_lstm"):
        # Log training parameters & lineage metadata
        mlflow.log_params(
            {
                "input_size": input_size,
                "hidden_size": train_config.hidden_size,
                "num_layers": train_config.num_layers,
                "dropout": train_config.dropout,
                "seq_length": train_config.seq_length,
                "batch_size": train_config.batch_size,
                "epochs": train_config.epochs,
                "lr": train_config.lr,
                "git_commit": git_hash,
                "dataset_fingerprint": data_bytes,
            }
        )

        train_model(
            model=model,
            train_loader=train_loader,
            val_loader=val_loader,
            config=train_config,
        )

    logger.info("Training pipeline execution successfully finalized.")


if __name__ == "__main__":
    main()
