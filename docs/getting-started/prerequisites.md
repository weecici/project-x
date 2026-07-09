# Prerequisites

Before setting up the Crypto Platform, ensure your environment meets these requirements.

## System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| **RAM** | 8 GB | 16 GB+ |
| **CPU** | 4 cores | 8+ cores |
| **Disk** | 20 GB free | 50 GB+ free |
| **OS** | Linux, macOS, WSL2 | Linux |

!!! warning "Memory Constraints"
    The platform is designed for a machine with **~7–8 GB usable RAM** (after IDE and browser). With 16 GB total RAM, you have comfortable headroom. With 8 GB total, close other applications before running the full stack.

## Software Requirements

### Required

| Tool | Version | Purpose | Install |
|------|---------|---------|---------|
| **Python** | 3.13+ | Runtime | [python.org](https://www.python.org/downloads/) or `pyenv install 3.13` |
| **uv** | Latest | Package manager | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| **Docker** | 24+ | Container runtime | [docs.docker.com](https://docs.docker.com/get-docker/) |
| **Docker Compose** | v2+ | Service orchestration | Included with Docker Desktop |

### Optional

| Tool | Version | Purpose | Install |
|------|---------|---------|---------|
| **just** | Latest | Command runner | `cargo install just` or `brew install just` |
| **pre-commit** | 3+ | Git hooks | `uv tool install pre-commit` |

## Verify Your Setup

Run these commands to confirm everything is installed:

```bash
python --version    # Should show Python 3.13.x
uv --version        # Should show uv 0.x+
docker --version    # Should show Docker 24+
docker compose version  # Should show Docker Compose v2.x
```

## GPU (Future Phases)

Phases 8–9 (ML training and serving) require an NVIDIA GPU with CUDA support.

| Spec | Value |
|------|-------|
| GPU | NVIDIA GeForce RTX 3050 Ti (or equivalent) |
| VRAM | 4 GB minimum |
| CUDA | 12.0+ |
| Driver | 525+ |

!!! note
    Phases 1–7 are **CPU-only**. You can develop and test the entire data pipeline without a GPU.
