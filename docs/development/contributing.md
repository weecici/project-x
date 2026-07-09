# Contributing

Guidelines for contributing to the Crypto Platform.

## Development Setup

1. **Clone and install:**

   ```bash
   git clone https://github.com/yourusername/crypto-platform.git
   cd crypto-platform
   uv sync
   ```

2. **Install pre-commit hooks:**

   ```bash
   pre-commit install
   ```

3. **Start infrastructure:**

   ```bash
   docker compose up -d
   ```

## Code Style

The project enforces strict code quality standards via pre-commit hooks.

### Linting & Formatting

- **Tool**: `ruff` (replaces black, flake8, isort)
- **Line length**: 88 characters
- **Quotes**: Double quotes
- **Import sorting**: `ruff` handles isort-style grouping automatically

```bash
# Auto-fix linting issues
uv run ruff check src/ --fix

# Auto-format
uv run ruff format src/
```

### Type Checking

- **Tool**: `mypy --strict`
- All public functions must have type annotations
- `ignore_missing_imports = true` for third-party stubs

```bash
uv run mypy src/
```

### Pre-commit Hooks

Every commit is automatically checked:

```yaml
- ruff check + ruff format
- mypy --strict
- Trailing whitespace removal
- End-of-file fixer
- YAML/TOML validation
- Merge conflict detection
- Large file prevention
```

If any hook fails, the commit is rejected. Fix the issues and re-commit.

## Project Structure

```
src/
├── __init__.py
├── utils/                  # Shared cross-phase utilities
├── ingestion/              # Phase 1: Live data pipeline
└── batch/                  # Phase 2: Batch processing
tests/
├── unit/                   # Fast, no Docker
├── integration/            # Docker required
└── e2e/                    # Full stack required
```

### Adding a New Module

1. Create the module in the appropriate package (`utils/`, `ingestion/`, or `batch/`)
2. Add type annotations to all public functions
3. Add a Google-style docstring to all public classes and functions
4. Update `__init__.py` with re-exports if the module is part of the public API
5. Write unit tests in `tests/unit/`
6. Run `pre-commit run` before committing

## Testing Requirements

### Before Submitting a PR

```bash
# All pre-commit hooks pass
pre-commit run

# Unit tests pass
uv run pytest tests/unit/ -v

# Type checking passes
uv run mypy src/
```

### Test Guidelines

- **Unit tests**: Required for all new code. No external dependencies.
- **Integration tests**: Required for I/O components (Kafka, S3, HTTP).
- **E2E tests**: Required for new pipeline features.
- **Coverage**: Target >90% for unit tests.

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(scope): add new feature
fix(scope): fix a bug
docs(scope): documentation changes
test(scope): add or update tests
chore(scope): maintenance tasks
refactor(scope): code restructuring
ci(scope): CI/CD changes
build(scope): build system changes
```

**Scope examples**: `ingestion`, `batch`, `utils`, `tests`, `docs`, `ci`

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with clear, focused commits
3. Ensure all pre-commit hooks and tests pass
4. Update documentation if changing public APIs
5. Submit PR with a clear description of changes

### PR Checklist

- [ ] Pre-commit hooks pass (`pre-commit run`)
- [ ] Unit tests pass (`uv run pytest tests/unit/ -v`)
- [ ] Type checking passes (`uv run mypy src/`)
- [ ] Documentation updated (if applicable)
- [ ] Commit messages follow Conventional Commits
