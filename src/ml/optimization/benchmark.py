"""Benchmarking harness for comparing model variants.

Evaluates size, latency (p50, p95), and validation accuracy for eager, compiled,
pruned, and quantized models. Outputs a formatted Markdown table.
"""

from __future__ import annotations

import tempfile
import time
import typing
from pathlib import Path

import mlflow
import numpy as np
import torch
from loguru import logger
from rich.console import Console
from rich.table import Table
from torch import nn
from torch.utils.data import DataLoader


def get_model_size_mb(model: nn.Module) -> float:
    """Calculate serialized model checkpoint size in Megabytes."""
    with tempfile.NamedTemporaryFile(suffix=".pt") as tmp:
        try:
            torch.save(model, tmp.name)
            return Path(tmp.name).stat().st_size / (1024 * 1024)
        except Exception:
            try:
                torch.save(model.state_dict(), tmp.name)
                return Path(tmp.name).stat().st_size / (1024 * 1024)
            except Exception as e:
                logger.warning("Could not calculate model size: {}", e)
                return 0.0


def measure_latency_ms(
    model: nn.Module,
    sample_input: torch.Tensor,
    reps: int = 100,
) -> tuple[float, float]:
    """Measure inference latency (p50, p95) on CPU in milliseconds."""
    model.eval()
    model_cpu = model.cpu()
    sample_cpu = sample_input.cpu()

    # Warm-up passes
    with torch.no_grad():
        for _ in range(10):
            _ = model_cpu(sample_cpu)

    latencies = []
    with torch.no_grad():
        for _ in range(reps):
            t0 = time.perf_counter()
            _ = model_cpu(sample_cpu)
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000.0)  # ms

    return float(np.percentile(latencies, 50)), float(np.percentile(latencies, 95))


def evaluate_accuracy(
    model: nn.Module, loader: DataLoader[tuple[torch.Tensor, torch.Tensor]]
) -> float:
    """Compute model accuracy on the provided DataLoader."""
    model.eval()
    model_cpu = model.cpu()
    correct = 0
    total = 0

    with torch.no_grad():
        for inputs, targets in loader:
            inputs, targets = inputs.cpu(), targets.cpu()
            outputs = model_cpu(inputs)
            _, predicted = outputs.max(1)
            total += targets.size(0)
            correct += predicted.eq(targets).sum().item()

    return correct / total if total > 0 else 0.0


@mlflow.trace
def run_optimization_benchmark(
    variants: dict[str, nn.Module],
    val_loader: DataLoader[tuple[torch.Tensor, torch.Tensor]],
    sample_batch_size: int = 64,
) -> str:
    """Benchmark all model variants and generate a summary Markdown table.

    Args:
        variants: Dict of name -> PyTorch nn.Module.
        val_loader: Validation dataset DataLoader.
        sample_batch_size: Batch size to use for latency testing.

    Returns:
        String representing the Markdown table.
    """
    logger.info("Executing comprehensive model optimization benchmark...")

    # Grab a sample input batch for latency profiling
    sample_input, _ = next(iter(val_loader))
    if len(sample_input) > sample_batch_size:
        sample_input = sample_input[:sample_batch_size]

    results: list[dict[str, typing.Any]] = []

    for name, model in variants.items():
        logger.info("Benchmarking variant: '{}'...", name)
        size_mb = get_model_size_mb(model)

        # Measure latency on CPU
        p50, p95 = measure_latency_ms(model, sample_input)

        # Evaluate validation accuracy
        acc = evaluate_accuracy(model, val_loader)

        results.append(
            {
                "variant": name,
                "size_mb": size_mb,
                "p50_latency_ms": p50,
                "p95_latency_ms": p95,
                "accuracy": acc,
            }
        )

    # Build Markdown table
    md_lines = [
        "# Model Optimization Benchmark Results",
        "",
        "| Variant | Size (MB) | p50 Latency (ms) | p95 Latency (ms) | Accuracy |",
        "|---|---|---|---|---|",
    ]
    for r in results:
        md_lines.append(
            f"| {r['variant']} | {r['size_mb']:.2f} MB | "
            f"{r['p50_latency_ms']:.2f} ms | {r['p95_latency_ms']:.2f} ms | "
            f"{r['accuracy']:.2%} |"
        )
    markdown_table = "\n".join(md_lines)

    # Print to console using Rich
    console = Console()
    table = Table(title="Model Optimization Benchmarks (Batch Size = 64)")
    table.add_column("Variant", style="cyan")
    table.add_column("Size (MB)", justify="right", style="magenta")
    table.add_column("p50 Latency", justify="right", style="green")
    table.add_column("p95 Latency", justify="right", style="green")
    table.add_column("Val Accuracy", justify="right", style="yellow")

    for r in results:
        table.add_row(
            r["variant"],
            f"{r['size_mb']:.2f} MB",
            f"{r['p50_latency_ms']:.2f} ms",
            f"{r['p95_latency_ms']:.2f} ms",
            f"{r['accuracy']:.2%}",
        )
    console.print(table)

    # Log metrics to MLflow active run
    if mlflow.active_run():
        import re

        for r in results:
            prefix = r["variant"].lower()
            prefix = re.sub(r"[^a-z0-9_-]", "_", prefix)
            prefix = re.sub(r"_+", "_", prefix).strip("_")
            mlflow.log_metric(f"{prefix}_size_mb", r["size_mb"])
            mlflow.log_metric(f"{prefix}_p50_latency_ms", r["p50_latency_ms"])
            mlflow.log_metric(f"{prefix}_p95_latency_ms", r["p95_latency_ms"])
            mlflow.log_metric(f"{prefix}_accuracy", r["accuracy"])

        # Log table as artifact
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
            f.write(markdown_table)
            f_path = f.name
        mlflow.log_artifact(f_path, artifact_path="benchmarks")
        logger.info(
            "Optimization benchmark metrics and artifact successfully logged to MLflow."
        )

    # Write to local docs folder
    docs_dir = Path("docs")
    docs_dir.mkdir(parents=True, exist_ok=True)
    local_path = docs_dir / "ml_optimization_benchmark.md"
    local_path.write_text(markdown_table, encoding="utf-8")
    logger.info("Saved local benchmark table to: {}", local_path)

    return markdown_table
