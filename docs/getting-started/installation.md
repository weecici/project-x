# Installation

## Requirements

- Python 3.8 or higher
- pip or Poetry

## From PyPI (Recommended)

Install the latest stable version:

```bash
pip install my-project
```

### Specific Version

```bash
pip install my-project==1.0.0
```

### With Optional Dependencies

```bash
# For data science features
pip install my-project[data-science]

# For all optional dependencies
pip install my-project[all]

# For development
pip install my-project[dev]
```

## From Source

Clone the repository and install in development mode:

```bash
git clone https://github.com/yourusername/my-project.git
cd my-project
pip install -e ".[dev]"
```

## Verify Installation

Confirm everything is working:

```python
>>> import my_project
>>> my_project.__version__
'1.0.0'
```

## Using Poetry

If you use Poetry:

```bash
poetry add my-project
```

Or add to `pyproject.toml`:

```toml
[tool.poetry.dependencies]
python = "^3.8"
my-project = "^1.0.0"
```

Then run:

```bash
poetry install
```

## Troubleshooting

### ModuleNotFoundError

Make sure you're using the correct Python interpreter:

```bash
which python
python --version
```

### Permission Errors

Use `--user` flag if you don't have system permissions:

```bash
pip install --user my-project
```

Or use a virtual environment (recommended):

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install my-project
```

## Next Steps

- [Quick Start](quickstart.md)
- [Basic Usage](../guides/basic-usage.md)
