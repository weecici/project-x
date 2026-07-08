# ADR-004: GPU Profile and Usage Plan

**Date:** 2026-07-05
**Status:** Accepted

## Hardware

| Spec | Value |
|---|---|
| GPU | NVIDIA GeForce RTX 3050 Ti Laptop |
| VRAM | 4 GB |
| CUDA Version | 13.3 |
| Compute Capability | sm_86 |
| Driver | 610.43.02 |

## Decision

Use the GPU in Phases 8–9 only. Phases 1–7 are CPU-only.

| Usage | Phase | Notes |
|---|---|---|
| PyTorch CUDA training | 8 | Compile for sm_86; small sequence models (LSTM ≤3 layers, hidden ≤256) fit in 4 GB VRAM |
| Numba CUDA JIT | 8 | Custom technical indicator loops (`@cuda.jit`) — benchmark vs CPU Numba and PySpark |
| Triton Inference Server | 9 | GPU backend (ONNX or TorchScript); compare GPU vs CPU serving latency |

## VRAM Budget

With 4 GB VRAM and sm_86:
- LSTM (3 layers, hidden 256, seq 60): ~50 MB model + ~200 MB activations — fits easily
- Transformer encoder (4 heads, 2 layers, d_model 128): ~10 MB model — fits easily
- Triton: load model + serving overhead ~500 MB — fits

## Consequences

| | |
|---|---|
| ✅ | GPU training significantly faster than CPU for sequence models |
| ✅ | Triton GPU serving produces real hardware latency numbers for the comparison table |
| ✅ | Numba CUDA JIT enables genuine CPU vs GPU indicator benchmark |
| ⚠️ | 4 GB VRAM rules out large models (GPT-style); keep models small and honest |
| ⚠️ | CUDA 13.3 is very recent; verify PyTorch CUDA 12.x wheel compatibility at install time |
