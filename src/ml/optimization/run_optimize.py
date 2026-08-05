"""CLI entrypoint for Phase 8 model optimization and benchmarking.

Loads the champion model from MLflow registry, applies torch.compile, ONNX
export, pruning, fine-tuning recovery, dynamic quantization, and benchmarks
performance size and latency.
"""

from __future__ import annotations

import copy
import os

import mlflow
import torch
from loguru import logger
from mlflow.tracking import MlflowClient
from torch.utils.data import DataLoader

from ml.features.config import FeatureConfig
from ml.features.dataset import CryptoDataset, load_gold_features, prepare_sequences
from ml.optimization.benchmark import run_optimization_benchmark
from ml.optimization.compile_model import compile_and_export_onnx
from ml.optimization.config import OptimizationConfig
from ml.optimization.prune_model import finetune_pruned_model, prune_model
from ml.optimization.quantize_model import quantize_dynamic


@mlflow.trace
def main() -> None:
    """Execute the model optimization and benchmarking pipeline."""
    opt_config = OptimizationConfig()
    feat_config = FeatureConfig()

    # Export MinIO variables to environment for boto3/MLflow client authentication
    os.environ["AWS_ACCESS_KEY_ID"] = feat_config.minio_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = feat_config.minio_secret_key
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = feat_config.minio_endpoint
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    logger.info("Initializing MLflow tracking...")
    mlflow.set_tracking_uri(opt_config.mlflow_tracking_uri)

    # Enable MLflow system metrics logging with a 1-second sampling interval
    mlflow.enable_system_metrics_logging()  # type: ignore[no-untyped-call]
    mlflow.set_system_metrics_sampling_interval(1)  # type: ignore[no-untyped-call]

    client = MlflowClient()

    # Load model from registry
    model_name = opt_config.model_name
    model_alias = opt_config.model_alias

    logger.info(
        "Loading model from registry (name: '{}', alias: '@{}')...",
        model_name,
        model_alias,
    )
    try:
        model = mlflow.pytorch.load_model(f"models:/{model_name}@{model_alias}")
        logger.info("Successfully loaded active champion model.")
    except Exception as e:
        logger.warning(
            "Could not load model with alias '@{}': {}. "
            "Falling back to latest version...",
            model_alias,
            e,
        )
        try:
            versions = client.search_model_versions(f"name='{model_name}'")
            if not versions:
                logger.error(
                    "No registered model versions found under '{}'. "
                    "Train a model first.",
                    model_name,
                )
                return
            latest_version = max(versions, key=lambda x: int(x.version)).version
            model = mlflow.pytorch.load_model(f"models:/{model_name}/{latest_version}")
            logger.info("Loaded latest registered model version: {}", latest_version)
        except Exception as ex:
            logger.error("Failed to load fallback model from registry: {}", ex)
            return

    logger.info("Loading feature dataset from MinIO to construct validation set...")
    try:
        df_features = load_gold_features(feat_config)
    except Exception as e:
        logger.error("Failed to load gold features: {}", e)
        return

    # Prepare datasets
    X, y, _ = prepare_sequences(
        df=df_features, seq_length=opt_config.seq_length, fit_scaler=False
    )
    n_samples = len(y)

    # 80/20 split matching training partition logic
    split_idx = int(n_samples * 0.8)
    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]

    train_loader = DataLoader(
        CryptoDataset(X_train, y_train), batch_size=64, shuffle=True
    )
    val_loader = DataLoader(CryptoDataset(X_val, y_val), batch_size=64, shuffle=False)

    sample_input, _ = next(iter(val_loader))

    # Apply optimizations in nested run context for tracking
    mlflow.set_experiment(feat_config.mlflow_experiment_name)
    with mlflow.start_run(run_name="model_optimization"):
        # 1. Compile model and export ONNX
        onnx_path = opt_config.output_dir / "crypto_model.onnx"
        compiled_model = compile_and_export_onnx(
            model=copy.deepcopy(model),
            sample_input=sample_input,
            onnx_output_path=onnx_path,
        )

        # 2. Prune model (copy to preserve original eager weights)
        pruned_model = prune_model(
            model=copy.deepcopy(model),
            prune_amount=opt_config.prune_amount,
        )

        # 3. Fine-tune pruned model to recover accuracy
        pruned_model = finetune_pruned_model(
            model=pruned_model,
            train_loader=train_loader,
            epochs=3,
            lr=1e-4,
        )

        # 4. Quantize pruned model dynamically to INT8 on CPU
        quantized_model = quantize_dynamic(model=copy.deepcopy(pruned_model))
        quantized_path = opt_config.output_dir / "quantized_model.pt"
        quantized_path.parent.mkdir(parents=True, exist_ok=True)
        torch.save(quantized_model.state_dict(), quantized_path)
        logger.info("Saved local quantized model checkpoint to: {}", quantized_path)

        # Run benchmarking on all variants
        variants = {
            "Baseline (Eager)": model,
            "JIT (Compiled)": compiled_model,
            "Pruned (30% Sparsity)": pruned_model,
            "Pruned + Quantized (INT8)": quantized_model,
        }

        run_optimization_benchmark(
            variants=variants,
            val_loader=val_loader,
        )

        # Log ONNX and quantized models as artifacts to MLflow active run
        mlflow.log_artifact(str(onnx_path), artifact_path="models")
        mlflow.log_artifact(str(quantized_path), artifact_path="models")
        logger.info("Successfully logged all optimization artifacts to MLflow.")

    logger.info("Model optimization benchmarking pipeline successfully complete.")


if __name__ == "__main__":
    main()
