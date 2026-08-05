"""Model training, validation loop, and MLflow logging/promotion logic."""

from __future__ import annotations

import mlflow
import statsd
import torch
from loguru import logger
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from torch import nn, optim
from torch.utils.data import DataLoader

from ml.training.config import TrainingConfig


@mlflow.trace
def train_model(
    model: nn.Module,
    train_loader: DataLoader[tuple[torch.Tensor, torch.Tensor]],
    val_loader: DataLoader[tuple[torch.Tensor, torch.Tensor]],
    config: TrainingConfig,
) -> nn.Module:
    """Train the sequence model and manage tracking and promotion.

    Args:
        model: PyTorch model module.
        train_loader: Dataloader for the training fold.
        val_loader: Dataloader for the validation fold.
        config: Training config parameters.

    Returns:
        The trained model.
    """
    device = torch.device(
        "cuda" if config.device == "cuda" and torch.cuda.is_available() else "cpu"
    )
    logger.info("Starting model training on target hardware: {}", device)
    model = model.to(device)

    # Initialize StatsD client for live metrics streaming to Prometheus/Grafana
    try:
        statsd_client = statsd.StatsClient(
            host=config.statsd_host,
            port=config.statsd_port,
            prefix=config.statsd_prefix,
        )
    except Exception as e:
        logger.warning("StatsD initialization failed (falling back): {}", e)
        statsd_client = None

    # Loss function and optimizer
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=config.lr)

    # Mixed precision training initialization (RTX 3050 Ti Laptop 4GB VRAM safety)
    use_amp = device.type == "cuda"
    scaler = torch.amp.grad_scaler.GradScaler("cuda", enabled=use_amp)

    best_val_accuracy = 0.0
    best_val_loss = float("inf")
    best_model_state = None

    for epoch in range(1, config.epochs + 1):
        model.train()
        train_loss = 0.0
        correct_train = 0
        total_train = 0

        for inputs, targets in train_loader:
            inputs, targets = inputs.to(device), targets.to(device)
            optimizer.zero_grad()

            with torch.amp.autocast_mode.autocast("cuda", enabled=use_amp):
                outputs = model(inputs)
                loss: torch.Tensor = criterion(outputs, targets)
            scaler.scale(loss).backward()  # type: ignore[no-untyped-call]
            scaler.step(optimizer)
            scaler.update()

            train_loss += loss.item() * inputs.size(0)
            _, predicted = outputs.max(1)
            total_train += targets.size(0)
            correct_train += predicted.eq(targets).sum().item()

        epoch_train_loss = train_loss / total_train
        epoch_train_acc = correct_train / total_train

        # Validation phase
        model.eval()
        val_loss = 0.0
        correct_val = 0
        total_val = 0
        predictions_all = []
        targets_all = []

        with torch.no_grad():
            for inputs, targets in val_loader:
                inputs, targets = inputs.to(device), targets.to(device)
                with torch.amp.autocast_mode.autocast("cuda", enabled=use_amp):
                    outputs = model(inputs)
                    loss = criterion(outputs, targets)

                val_loss += loss.item() * inputs.size(0)
                _, predicted = outputs.max(1)
                total_val += targets.size(0)
                correct_val += predicted.eq(targets).sum().item()

                predictions_all.extend(predicted.cpu().numpy())
                targets_all.extend(targets.cpu().numpy())

        epoch_val_loss = val_loss / total_val
        epoch_val_acc = correct_val / total_val

        logger.info(
            "Epoch {}/{} - Train Loss: {:.4f}, Acc: {:.2%} | "
            "Val Loss: {:.4f}, Acc: {:.2%}",
            epoch,
            config.epochs,
            epoch_train_loss,
            epoch_train_acc,
            epoch_val_loss,
            epoch_val_acc,
        )

        # Log epoch level metrics to MLflow and StatsD
        mlflow.log_metric("train_loss", epoch_train_loss, step=epoch)
        mlflow.log_metric("train_accuracy", epoch_train_acc, step=epoch)
        mlflow.log_metric("val_loss", epoch_val_loss, step=epoch)
        mlflow.log_metric("val_accuracy", epoch_val_acc, step=epoch)

        if statsd_client:
            statsd_client.gauge("epoch_train_loss", epoch_train_loss)
            statsd_client.gauge("epoch_val_loss", epoch_val_loss)
            statsd_client.gauge("epoch_val_accuracy", epoch_val_acc)

        # Save checkpoint of best validation accuracy
        if epoch_val_acc > best_val_accuracy:
            best_val_accuracy = epoch_val_acc
            best_val_loss = epoch_val_loss
            best_model_state = {k: v.cpu() for k, v in model.state_dict().items()}

    logger.info(
        "Training completed. Loading best checkpoint (Val Acc: {:.2%})",
        best_val_accuracy,
    )
    if best_model_state:
        model.load_state_dict({k: v.to(device) for k, v in best_model_state.items()})

    # Log best parameters & metrics summary
    mlflow.log_metric("best_val_accuracy", best_val_accuracy)
    mlflow.log_metric("best_val_loss", best_val_loss)

    # Infer Model Schema Signature for deployment safety
    sample_input, _ = next(iter(val_loader))
    sample_output = model(sample_input.to(device))
    signature = infer_signature(
        sample_input.cpu().numpy(),
        sample_output.detach().cpu().numpy(),
    )

    # Log model checkpoint with signature to the artifact store
    logger.info("Logging model to MLflow artifact registry...")
    mlflow.pytorch.log_model(
        pytorch_model=model,
        artifact_path="model",
        signature=signature,
        input_example=sample_input.cpu().numpy(),
        serialization_format="pickle",
    )

    # Register model in MLflow model registry and manage Champion-Challenger aliases
    try:
        active_run = mlflow.active_run()
        if active_run is None:
            raise RuntimeError("No active MLflow run found.")
        run_id = active_run.info.run_id
        model_uri = f"runs:/{run_id}/model"

        logger.info("Registering model version under name: '{}'", config.model_name)
        model_details = mlflow.register_model(
            model_uri=model_uri, name=config.model_name
        )
        client = MlflowClient()

        # Set Challenger pointer on new model version
        logger.info(
            "Assigning '@challenger' alias to version {}", model_details.version
        )
        client.set_registered_model_alias(
            name=config.model_name,
            alias="challenger",
            version=model_details.version,
        )

        # Add metadata tag containing model performance metric
        client.set_model_version_tag(
            name=config.model_name,
            version=model_details.version,
            key="val_accuracy",
            value=str(best_val_accuracy),
        )

        # Get current Champion version and evaluate promotion conditions
        try:
            champion_mv = client.get_model_version_by_alias(
                config.model_name, "champion"
            )
            champion_acc = float(champion_mv.tags.get("val_accuracy", "0.0"))
            logger.info(
                "Current '@champion' version {} accuracy: {:.2%}",
                champion_mv.version,
                champion_acc,
            )
        except Exception:
            champion_mv = None
            champion_acc = 0.0
            logger.info("No existing '@champion' model version detected.")

        if best_val_accuracy > champion_acc:
            logger.info(
                "Challenger accuracy outperforms current Champion. Promoting..."
            )
            client.set_registered_model_alias(
                name=config.model_name,
                alias="champion",
                version=model_details.version,
            )
            # Remove challenger alias as it has been promoted
            client.delete_registered_model_alias(config.model_name, "challenger")
            logger.info(
                "New model version {} is now promoted to '@champion'.",
                model_details.version,
            )
        else:
            logger.info(
                "Challenger does not outperform Champion. "
                "Retaining version {} as '@challenger'.",
                model_details.version,
            )

    except Exception as e:
        logger.error("Failed to register model or manage registry aliases: {}", e)

    return model
