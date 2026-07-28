"""Runtime configuration for Airflow orchestration and OpenMetadata governance.

Reads environment variables or `.env` using pydantic-settings.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class OrchestrationConfig(BaseSettings):
    """Configuration for Airflow workflow orchestrator."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    airflow_url: str = Field(
        default="http://localhost:8085",
        description="HTTP URL for Airflow webserver.",
    )
    airflow_user: str = Field(default="airflow")
    airflow_password: str = Field(default="airflow")
    airflow_dags_folder: Path = Field(
        default=Path("src/orchestration/dags"),
        description="Path to local Airflow DAG directory.",
    )


class GovernanceConfig(BaseSettings):
    """Configuration for OpenLineage and OpenMetadata lineage emission."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    openlineage_url: str = Field(
        default="http://localhost:8585/api/v1/openlineage",
        description="OpenLineage metadata collector HTTP endpoint.",
    )
    openlineage_namespace: str = Field(
        default="crypto-platform",
        description="Namespace for lineage events.",
    )
    openmetadata_url: str = Field(
        default="http://localhost:8585",
        description="OpenMetadata UI/API endpoint.",
    )
    lineage_output_dir: Path = Field(
        default=Path(".exports"),
        description="Directory to output exported lineage manifests.",
    )
