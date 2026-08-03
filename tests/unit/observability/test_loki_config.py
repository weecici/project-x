"""Unit tests for Loki and AlertManager YAML configurations."""

from pathlib import Path

import yaml


def test_loki_config_validity() -> None:
    loki_file = Path("infra/observability/loki/loki-config.yaml")
    assert loki_file.exists(), "loki-config.yaml must exist"

    with open(loki_file, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    assert isinstance(config, dict)
    assert config.get("auth_enabled") is False
    assert "server" in config
    assert "schema_config" in config
    assert "limits_config" in config


def test_alertmanager_config_validity() -> None:
    am_file = Path("infra/observability/alertmanager/alertmanager.yml")
    assert am_file.exists(), "alertmanager.yml must exist"

    with open(am_file, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    assert isinstance(config, dict)
    assert "route" in config
    assert "receivers" in config
