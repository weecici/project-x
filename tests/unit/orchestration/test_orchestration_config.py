"""Unit tests for OrchestrationConfig and GovernanceConfig."""

from __future__ import annotations

from pathlib import Path

import pytest

from orchestration.config import GovernanceConfig, OrchestrationConfig


class TestOrchestrationConfig:
    """Test defaults and overrides for OrchestrationConfig."""

    def test_default_config(self) -> None:
        """Verify Airflow defaults."""
        config = OrchestrationConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.airflow_url == "http://localhost:8085"
        assert config.airflow_user == "airflow"
        assert config.airflow_password == "airflow"
        assert config.airflow_dags_folder == Path("src/orchestration/dags")

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify environment variable overrides."""
        monkeypatch.setenv("AIRFLOW_URL", "http://airflow-server:8085")
        monkeypatch.setenv("AIRFLOW_USER", "admin")

        config = OrchestrationConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.airflow_url == "http://airflow-server:8085"
        assert config.airflow_user == "admin"


class TestGovernanceConfig:
    """Test defaults and overrides for GovernanceConfig."""

    def test_default_config(self) -> None:
        """Verify Governance defaults."""
        config = GovernanceConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.openlineage_namespace == "crypto-platform"
        assert config.openmetadata_url == "http://localhost:8585"
        assert config.lineage_output_dir == Path(".exports")

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify environment variable overrides."""
        monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "crypto-prod")

        config = GovernanceConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.openlineage_namespace == "crypto-prod"
