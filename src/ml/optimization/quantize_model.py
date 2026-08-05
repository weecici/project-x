"""Model quantization functions."""

from __future__ import annotations

import mlflow
import torch
from loguru import logger
from torch import nn


@mlflow.trace
def quantize_dynamic(model: nn.Module) -> nn.Module:
    """Apply dynamic INT8 quantization targeting LSTM and Linear layers.

    Args:
        model: Trained or pruned PyTorch model (runs on CPU).

    Returns:
        Quantized PyTorch model.
    """
    logger.info("Applying dynamic INT8 quantization targeting {nn.LSTM, nn.Linear}...")

    # Dynamic quantization requires the model to be on CPU.
    # It converts floating point weights to 8-bit integers.
    model_cpu = model.cpu()
    quantized_model = torch.ao.quantization.quantize_dynamic(  # type: ignore[no-untyped-call]
        model=model_cpu,
        qconfig_spec={nn.LSTM, nn.Linear},
        dtype=torch.qint8,
    )

    logger.info("Dynamic quantization completed successfully.")
    assert isinstance(quantized_model, nn.Module)
    return quantized_model
