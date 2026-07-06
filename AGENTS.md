# AGENTS.md

## Project

Crypto market intelligence platform: ingest live + historical crypto data, build a lakehouse, serve analytics via OLAP + semantic layer, train/optimize/serve a price-movement model. See `GUIDE.md` for the full 311-line blueprint.

**Current state**: Pre-build planning phase. No application code, dependencies, tests, or infrastructure configured yet. The repo contains only planning docs and wiki.

## Machine Spec
- Total RAM: 14 GB / **~7–8 GB usable** (IDE + browser consume ~6 GB at rest)
- CPU: 16 cores
- GPU: NVIDIA RTX 3050 Ti Laptop — 4 GB VRAM, CUDA 13.3, sm_86 (used in Phases 8–9)
- Disk: ~56 GB free

## Toolchain

- Docker and Docker Compose: yes
- Kubernetes: temporarily no

- **Python 3.13** (pinned in `.python-version`)
- **Package manager**: `uv` — use `uv add`, `uv run`, `uv sync`
- **Linting + formatting**: `ruff` (replaces black, flake8, isort) — 88-char lines, double quotes
- **Type checking**: `mypy --strict`
- **Test framework**: `pytest` with `pytest-asyncio` (asyncio_mode=auto) + `testcontainers`
- **Pre-commit hooks**: `ruff` + `mypy` enforced on every commit

## Commands

```bash
# Infrastructure
docker compose up -d              # start Kafka, MinIO, kafka-ui
docker compose stop kafka-ui      # free RAM when not needed
docker compose ps                 # check service health

# Lint + type check
uv run ruff check .               # lint
uv run ruff format .              # format
uv run mypy .                     # type check

# Tests
uv run pytest tests/unit/ -v                # fast, no Docker required
uv run pytest tests/integration/ -v        # requires Docker daemon
uv run pytest tests/e2e/ -v -m e2e         # requires full compose stack + running processes

# Run ingestion (Phase 1)
uv run python ingestion/run_producer.py     # Binance WS → Kafka
uv run python ingestion/run_lake_writer.py  # Kafka → MinIO bronze

# Or via project scripts
uv run run-producer
uv run run-lake-writer
```

## Architecture

Phase 1 directory structure (built — see `wiki/structure/project-structure.md` for full layout):

```
ingestion/       → Binance WS → Kafka producer + Kafka → MinIO lake writer
tests/           → unit / integration (testcontainers) / e2e
wiki/decisions/  → Architecture Decision Records (ADR-001 through ADR-004)
```

**Planned** (future phases):
```
batch/           → PySpark backfill + feature history (Phase 2)
dbt_project/     → silver → gold SQL models (Phase 3)
streaming/       → Flink windowed aggregation jobs (Phase 4)
ml/              → features, training, optimization, serving (Phases 8–9)
orchestration/   → Airflow DAGs (Phase 6)
observability/   → Prometheus + Grafana configs (Phase 7)
infra/           → Docker Swarm + K8s manifests (Phase 10)
```

Follow the 10-phase build order in `wiki/structure/phase.md`.

## Wiki

`wiki/` is LLM-owned documentation. Read it for context; the LLM creates and updates pages.

- `wiki/INDEX.md` — content catalog of all wiki pages (read this first to find relevant docs)
- `wiki/LOG.md` — chronological record of wiki changes
- `wiki/architecture/` — system architecture and component breakdown
- `wiki/structure/` — project structure and phased build order

When answering project questions, check `wiki/INDEX.md` first, then drill into referenced pages.

## Coding standards

These instructions define how to structure, format, and comment Python code.
Follow them for every file you write or edit, so output stays consistent
regardless of task size. Based on PEP 8, PEP 257, PEP 484, and common
conventions from Google/`black`/`ruff` tooling.

### 1. File Layout (top to bottom order)

Every `.py` file follows this order, with **one blank line** between
sections (two blank lines between top-level classes/functions):

```python
"""Module docstring: one-line summary of what this module does.

Optional longer description if the module's purpose isn't obvious
from the summary alone.
"""

from __future__ import annotations  # if using deferred type hints

# 1. Standard library imports
import os
import sys
from dataclasses import dataclass
from pathlib import Path

# 2. Third-party imports
import numpy as np
import requests
from pydantic import BaseModel

# 3. Local/first-party imports
from myproject.core import utils
from myproject.models import User

# Module-level constants (UPPER_SNAKE_CASE)
DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3

# Module-level logger (if used)
logger = logging.getLogger(__name__)


class MyClass:
    ...


def my_function():
    ...


def main() -> None:
    ...


if __name__ == "__main__":
    main()
```

#### Import rules
- Group imports into exactly three blocks in this order: **stdlib → third-party → local**, each separated by one blank line.
- Within each block, sort alphabetically (`isort` defaults: `import` statements before `from ... import` statements, then alphabetical).
- Use absolute imports (`from myproject.core import utils`), not relative (`from ..core import utils`), unless the project already establishes relative-import conventions.
- Never use wildcard imports (`from module import *`).
- Import modules/objects you actually use — no dead imports. Don't import something just to re-export it without an explicit `__all__`.
- One import per line for `import x` style; multiple names can share one `from x import a, b, c` line if they fit and are related.

### 2. Function & Class Organization

**Order within a class:**
1. Class docstring
2. Class-level constants/attributes
3. `__init__`
4. Dunder methods (`__repr__`, `__eq__`, etc.)
5. Properties
6. Public methods
7. Private methods (prefixed `_`)
8. Static/class methods (grouped near related public methods, or at the end)

**Order within a module:** constants → exceptions → dataclasses/models → helper functions → main classes → `main()`/entry point → `if __name__ == "__main__"` guard.

**Function design rules:**
- Single responsibility: a function does one thing. If you need "and" to describe it, split it.
- Keep functions short enough to read on one screen (~40 lines is a soft ceiling; longer is a signal to extract helpers).
- Use type hints on every function signature (parameters and return type), including `-> None` for procedures.
- Prefer pure functions (no hidden side effects) where practical; if a function mutates state or does I/O, make that obvious from its name (`save_`, `fetch_`, `update_`).
- Avoid more than 3–4 positional parameters; use keyword-only arguments (`*,`) for anything beyond that or anything easy to misorder.
- Avoid mutable default arguments (`def f(x=[])` → use `None` and initialize inside).

### 3. Naming Conventions (PEP 8)

| Element | Convention | Example |
|---|---|---|
| Module/package | `lower_snake_case`, short | `data_loader.py` |
| Class | `PascalCase` | `UserAccount` |
| Function/method | `lower_snake_case` | `calculate_total()` |
| Variable | `lower_snake_case` | `user_count` |
| Constant | `UPPER_SNAKE_CASE` | `MAX_RETRIES` |
| Private (internal use) | leading underscore | `_helper()` |
| "Really don't touch" | leading double underscore | `__internal_state` |
| Type variable | `PascalCase`, short | `T`, `KeyType` |

Names should be descriptive over clever; avoid single-letter names except for trivial loop counters (`i`, `j`) or well-known math contexts.

### 4. Docstrings & Comments

Use **Google-style docstrings** by default (unless the project already uses NumPy-style or reST — match what's there):

```python
def fetch_user(user_id: int, *, include_inactive: bool = False) -> User | None:
    """Fetch a user by ID.

    Args:
        user_id: The unique identifier of the user.
        include_inactive: Whether to include deactivated accounts.

    Returns:
        The matching User, or None if no user was found.

    Raises:
        ValueError: If user_id is not positive.
    """
```

**Rules:**
- Every public module, class, and function gets a docstring. Private helpers (`_foo`) only need one if their behavior isn't obvious from the name + signature.
- Docstring first line is a short imperative summary ("Fetch a user...", not "Fetches a user..." or "This function fetches...").
- Use inline comments (`#`) sparingly, only to explain **why**, not **what** — the code should already say what it does.
  - Bad: `x += 1  # increment x`
  - Good: `x += 1  # offset for 1-indexed API response`
- Don't leave commented-out code in final output. Remove it or explain in a `TODO:`/`FIXME:` if intentional.
- Use `# TODO(name): description` for follow-up work when relevant.

### 5. Formatting & Tooling

- Follow PEP 8 line length: 88 characters (Black's default) unless the project specifies otherwise (e.g. 79 or 100).
- Use double quotes for strings unless the project convention is single quotes; be consistent within a file.
- Assume/format as if these tools will run: **Black** (formatting), **Ruff** or **Flake8** (linting), **mypy** (type checking), **isort** (import sorting, though Ruff can replace it). Write code that would pass these with no warnings.
- Use f-strings for string interpolation, not `%` or `.format()`.
- Use `pathlib.Path` over `os.path` for filesystem paths in new code.

### 6. Error Handling

- Catch specific exceptions, never bare `except:`.
- Don't swallow exceptions silently — log or re-raise.
- Define custom exceptions for domain-specific errors, inheriting from a project-level base exception when one exists.
- Use context managers (`with`) for resources (files, connections, locks).

```python
class UserNotFoundError(Exception):
    """Raised when a requested user does not exist."""


try:
    user = fetch_user(user_id)
except UserNotFoundError:
    logger.warning("User %s not found", user_id)
    raise
```

### 7. Testing Conventions (when writing tests)

- Test files: `test_<module>.py`, mirroring the source module's name.
- Test functions: `test_<behavior_being_tested>()`, e.g. `test_fetch_user_returns_none_when_missing`.
- Structure each test as **Arrange → Act → Assert**, with blank lines separating the three (or comments if the sections aren't obvious).
- One logical assertion focus per test; use `pytest.mark.parametrize` for variations instead of copy-pasted tests.

### 8. Project Structure (for multi-file projects)

```
project_root/
├── src/
│   └── myproject/
│       ├── __init__.py
│       ├── core/
│       ├── models/
│       └── utils/
├── tests/
├── pyproject.toml
├── README.md
└── .gitignore
```

- Use `pyproject.toml` for dependencies/config (not bare `setup.py`/`requirements.txt`) unless the existing project uses the latter.
- Keep `__init__.py` files minimal — re-export the public API, don't put logic there.

### 9. General Consistency Rules

- **Match existing project conventions first.** If a codebase already picks single quotes, 100-char lines, or NumPy docstrings, follow the codebase over this document.
- Don't mix styles within one file (e.g., some functions typed, others not).
- Prefer explicit over implicit (`from x import y` over deep chained attribute access set up via `*`).
- When editing existing code, preserve the surrounding style even if it differs slightly from this guide — consistency within a file beats external purity.
