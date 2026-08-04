# Standard Airflow Dockerfile utilizing uv for fast, reproducible dependency syncing
FROM apache/airflow:3.3.0-python3.13

USER root

# Install OpenJDK 17 for PySpark Java gateway support
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install uv binary from official Astral image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /opt/airflow/app

# Copy dependency manifests and project metadata required for editable package build
COPY pyproject.toml uv.lock README.md ./
COPY src ./src

# Install project and dependencies into system Python environment as root
RUN uv pip install --system --no-cache -e .

# Create deterministic SimpleAuthManager password configuration
RUN echo '{"admin": "admin"}' > /tmp/simple_auth_passwords.json && chown airflow:0 /tmp/simple_auth_passwords.json

# Switch back to non-root airflow user for security and container execution
USER airflow
