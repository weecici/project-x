# Installation

## Clone the Repository

```bash
git clone https://github.com/yourusername/crypto-platform.git
cd crypto-platform
```

## Install Dependencies

The project uses [uv](https://docs.astral.sh/uv/) as its package manager.

```bash
# Install all runtime + dev dependencies
uv sync
```

This creates a `.venv/` directory and installs everything defined in `pyproject.toml`.

## Install Pre-commit Hooks

```bash
pre-commit install
```

This sets up automatic linting (ruff) and type checking (mypy) on every commit.

## Start Infrastructure

The platform requires Apache Kafka and MinIO (S3-compatible storage) running locally via Docker:

```bash
docker compose up -d
```

Verify all services are healthy:

```bash
docker compose ps
```

Expected output:

```
NAME                    STATUS          PORTS
platform-kafka          running         0.0.0.0:9094->9094/tcp
platform-kafka-ui       running         0.0.0.0:8080->8080/tcp
platform-minio          running         0.0.0.0:9000->9000/tcp, 0.0.0.0:9001->9001/tcp
```

## Configure Environment

Copy the example environment file and adjust if needed:

```bash
cp .env.example .env
```

The defaults work out of the box for local development. See the [Configuration Guide](../guides/configuration.md) for all available options.

## Verify Installation

Run the unit tests to confirm everything works:

```bash
uv run pytest tests/unit/ -v
```

All tests should pass without requiring Docker or any external services.

## Troubleshooting

### Docker services won't start

Ensure Docker is running:

```bash
docker info
```

If using Docker Desktop, wait for the engine to fully start before running `docker compose up`.

### Port conflicts

If ports 9094, 8080, 9000, or 9001 are already in use, stop the conflicting service or modify `docker-compose.yaml`.

### `uv sync` fails

Ensure you have Python 3.13 installed and available:

```bash
uv python list
```

If 3.13 is not installed:

```bash
uv python install 3.13
```
