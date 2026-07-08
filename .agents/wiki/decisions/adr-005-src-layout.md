# ADR-005: Adoption of the `src/` Directory Layout

**Date:** 2026-07-07
**Status:** Accepted

## Context

Initially, the project placed python source modules directly at the root of the workspace (e.g. `ingestion/`). While simple for initial bootstrapping, a flat root layout has several disadvantages as projects grow:
1. **Accidental Imports**: Tests or scripts can import the local directory code directly even if the package is not installed in the active environment. This hides packaging issues (like missing `pyproject.toml` dependencies or incorrect entrypoint mappings) until they fail in production/CI.
2. **Namespace Clutter**: The root of the repository becomes cluttered with source directories, tooling configurations, documentation, infrastructure manifests, and temporary build outputs.
3. **Packaging Standard**: Modern Python packaging standards (PEP 517/518, `uv`, `poetry`, `setuptools`) recommend the `src/` layout to ensure that tests always run against the installed/compiled distribution of the package, rather than the local source code.

## Decision

Reorganize the repository to adopt the standard `src/` layout:
1. Move the `ingestion/` module to `src/ingestion/`.
2. Move future Python execution modules (`batch`, `streaming`, `ml`, `orchestration`) under `src/`.
3. Configure the `pyproject.toml` build system with `uv_build` as the backend and namespace packaging enabled (`namespace = true`) to support editable installs (`uv sync`).
4. Update imports in test files and scripts to resolve the modules correctly via standard imports.

## Consequences

| | |
|---|---|
| ✅ | Prevents accidental imports of local files; guarantees the code is packaged correctly |
| ✅ | Kept root workspace clean by isolating all Python package modules under a single `src/` directory |
| ✅ | Aligns with standard Python packaging best practices and tools like `uv` |
| ✅ | Simplifies python path management (`PYTHONPATH`) across test runners and tools |
| ⚠️ | Requires installing the package in editable mode (`uv sync` or `uv run`) for local development scripts |
