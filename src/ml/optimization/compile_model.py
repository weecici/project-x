"""Model compilation and ONNX serialization functions."""

from __future__ import annotations

from pathlib import Path

import mlflow
import torch
from loguru import logger
from torch import nn


@mlflow.trace
def compile_and_export_onnx(
    model: nn.Module,
    sample_input: torch.Tensor,
    onnx_output_path: Path,
) -> nn.Module:
    """Compile the PyTorch model using torch.compile and export it to ONNX.

    Args:
        model: Trained eager-mode PyTorch model.
        sample_input: Sample tensor representing a batch input of shape (B, S, F).
        onnx_output_path: Local filesystem target path to write the ONNX model.

    Returns:
        The JIT compiled PyTorch model.
    """
    logger.info("Initializing torch.compile optimization (mode='reduce-overhead')...")
    # torch.compile generates optimized kernel code dynamically for the runtime
    # execution path
    compiled_model = torch.compile(model, mode="reduce-overhead")

    # Benchmarking warm-up passes
    device = next(model.parameters()).device
    sample_device = sample_input.to(device)

    logger.info("Running JIT compiler warm-up passes...")
    for _ in range(5):
        _ = compiled_model(sample_device)

    # ONNX Serialization
    # Exporting the model to ONNX decouples execution from the Python runtime,
    # supporting Triton serving.
    logger.info("Exporting model to ONNX format (opset=17) at: {}", onnx_output_path)
    onnx_output_path.parent.mkdir(parents=True, exist_ok=True)

    # Export the eager-mode model for structural portability
    model.eval()
    with torch.no_grad():
        torch.onnx.export(
            model=model.cpu(),
            args=(sample_input.cpu(),),
            f=str(onnx_output_path),
            export_params=True,
            opset_version=17,
            do_constant_folding=True,
            input_names=["input"],
            output_names=["output"],
            dynamic_axes={
                "input": {0: "batch_size"},
                "output": {0: "batch_size"},
            },
        )

    # Put model back to its original device
    model.to(device)

    logger.info(
        "ONNX model successfully serialized. Size: {:.2f} MB",
        onnx_output_path.stat().st_size / (1024 * 1024),
    )
    import typing

    return typing.cast(nn.Module, compiled_model)
