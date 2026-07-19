"""Unit tests for BiExporterConfig."""

from __future__ import annotations

from pathlib import Path

import pytest

from olap.config import BiExporterConfig


class TestBiExporterConfig:
    """Verify default values and settings overrides of BiExporterConfig."""

    def test_default_config(self) -> None:
        """Verify default Cube values and export directory path."""
        config = BiExporterConfig(_env_file=None)  # type: ignore[call-arg]

        assert config.cube_api_url == "http://localhost:4000"
        assert config.cube_api_secret == "change_me_in_production"
        assert config.export_output_dir == Path(".exports")
        assert config.google_service_account_json is None
        assert config.google_sheet_name == "Crypto Platform Analytics"

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify overrides work via environment variables."""
        monkeypatch.setenv("CUBE_API_URL", "http://cube-api:4000")
        monkeypatch.setenv("EXPORT_OUTPUT_DIR", "/tmp/bi_exports")
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", "/keys/sa.json")
        monkeypatch.setenv("GOOGLE_SHEET_NAME", "Prod Crypto Sheets")

        config = BiExporterConfig(_env_file=None)  # type: ignore[call-arg]
        assert config.cube_api_url == "http://cube-api:4000"
        assert config.export_output_dir == Path("/tmp/bi_exports")
        assert config.google_service_account_json == "/keys/sa.json"
        assert config.google_sheet_name == "Prod Crypto Sheets"
