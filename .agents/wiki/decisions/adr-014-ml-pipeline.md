# ADR 014: Machine Learning Retraining Pipeline & Model Optimization

## Context
In Phase 8, we build a local-first Crypto Market Intelligence ML Pipeline. The model must predict the t+1 price direction (binary classification: 1 for up, 0 for down) based on sequence patterns of 60 historical lookback windows (~1 hour of 1-minute klines).
We need:
1. High-throughput feature engineering.
2. Fast numerical calculations.
3. Structured tracking and versioning.
4. Model optimization for target deployment (RTX 3050 Ti VRAM limit).

## Decision
We adopt a unified machine learning stack utilizing:
- **PySpark Vectorized Pandas UDFs & `pandas-ta`**: For calculating features on historical klines.
- **Numba JIT compilation**: To accelerate custom feature metrics (EMA, RSI, MACD) in Python, achieving massive speedups (up to 97x) compared to pure Python loops.
- **PyTorch LSTM Classifier**: Pinned to GPU (`cuda:0` / RTX 3050 Ti laptop GPU).
- **MLflow Tracking & Model Registry**: Logging experiments, metrics, parameters, and managing Champion-Challenger aliases.
- **Multi-tiered Model Optimization**:
  - **Dynamic JIT Compilation (`torch.compile`)**: Fuses PyTorch execution blocks to reduce Python interpreter overhead.
  - **ONNX Export**: Serializes the model in standard ONNX format, decoupling execution from Python.
  - **Global L1 Unstructured Pruning (30%)**: Compacts model parameters by setting weights with small magnitudes to zero, followed by short-epoch fine-tuning recovery.
  - **Dynamic INT8 Quantization (`torch.ao.quantization`)**: Quantizes weights from Float32 to Int8 to reduce footprint from 5.07MB to 1.29MB (approx. 74% storage reduction).

## Consequences
- PySpark distributes group-by computations per symbol cleanly.
- MLflow automates versioning and maintains tracking for deployment.
- The quantized/pruned model runs efficiently on edge or memory-restricted servers.
