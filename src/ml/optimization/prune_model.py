"""Model pruning implementation and fine-tuning recovery logic."""

from __future__ import annotations

import mlflow
import torch
import torch.nn.utils.prune as prune
from loguru import logger
from torch import nn, optim
from torch.utils.data import DataLoader


@mlflow.trace
def prune_model(model: nn.Module, prune_amount: float = 0.3) -> nn.Module:
    """Apply global L1 unstructured pruning to weights of Linear and LSTM modules.

    Args:
        model: Trained PyTorch model.
        prune_amount: Fraction of weights to prune (e.g. 0.3).

    Returns:
        Pruned model with masks permanently removed/applied.
    """
    logger.info(
        "Applying global L1 unstructured pruning (amount={:.0%})...", prune_amount
    )

    parameters_to_prune = []
    # Identify weights within linear and LSTM layers to include in global pruning pool
    for _, module in model.named_modules():
        if isinstance(module, (nn.Linear, nn.LSTM)):
            for param_name, _ in module.named_parameters():
                if "weight" in param_name:
                    parameters_to_prune.append((module, param_name))

    if not parameters_to_prune:
        logger.warning("No parameters found matching pruning criteria.")
        return model

    # Apply global unstructured pruning across all parameters in the pool
    prune.global_unstructured(  # type: ignore[no-untyped-call]
        parameters_to_prune,
        pruning_method=prune.L1Unstructured,
        amount=prune_amount,
    )

    # Remove the reparameterization hooks to make the pruning permanent
    # (replaces weight parameter with pruned values, removing mask dependency)
    for module, param_name in parameters_to_prune:
        prune.remove(module, param_name)  # type: ignore[no-untyped-call]

    # Count non-zero parameters to verify sparsity
    total_weights = 0
    zero_weights = 0
    for module, param_name in parameters_to_prune:
        w = getattr(module, param_name)
        total_weights += w.numel()
        zero_weights += int(torch.sum(w == 0).item())

    logger.info(
        "Pruning completed. Sparsity verified: {:.2%} ({}/{} zero weights)",
        zero_weights / total_weights,
        zero_weights,
        total_weights,
    )
    return model


def finetune_pruned_model(
    model: nn.Module,
    train_loader: DataLoader[tuple[torch.Tensor, torch.Tensor]],
    epochs: int = 5,
    lr: float = 1e-4,
) -> nn.Module:
    """Fine-tune the model post-pruning to recover any accuracy loss.

    Args:
        model: Pruned PyTorch model.
        train_loader: Training DataLoader.
        epochs: Fine-tuning epochs.
        lr: Fine-tuning learning rate (should be lower than training rate).

    Returns:
        Fine-tuned model.
    """
    logger.info("Starting fine-tuning recovery for {} epochs (lr={})...", epochs, lr)
    device = next(model.parameters()).device
    model.train()
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)

    for epoch in range(1, epochs + 1):
        total_loss = 0.0
        correct = 0
        total = 0

        for inputs, targets in train_loader:
            inputs, targets = inputs.to(device), targets.to(device)
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()

            total_loss += loss.item() * inputs.size(0)
            _, predicted = outputs.max(1)
            total += targets.size(0)
            correct += predicted.eq(targets).sum().item()

        logger.info(
            "Fine-tune Epoch {}/{} - Loss: {:.4f}, Acc: {:.2%}",
            epoch,
            epochs,
            total_loss / total,
            correct / total,
        )

    return model
