"""Unit tests for Grafana datasources and dashboards provisioning."""

import json
from pathlib import Path

import yaml


def test_grafana_datasources_provisioning() -> None:
    ds_file = Path(
        "infra/observability/grafana/provisioning/datasources/datasources.yml"
    )
    assert ds_file.exists(), "datasources.yml must exist"

    with open(ds_file, encoding="utf-8") as f:
        content = yaml.safe_load(f)

    assert isinstance(content, dict)
    assert content.get("apiVersion") == 1
    assert "datasources" in content
    ds_names = [d["name"] for d in content["datasources"]]
    assert "Prometheus" in ds_names
    assert "Loki" in ds_names


def test_grafana_dashboards_provisioning() -> None:
    dash_file = Path(
        "infra/observability/grafana/provisioning/dashboards/dashboards.yml"
    )
    assert dash_file.exists(), "dashboards.yml must exist"

    with open(dash_file, encoding="utf-8") as f:
        content = yaml.safe_load(f)

    assert isinstance(content, dict)
    assert content.get("apiVersion") == 1
    assert "providers" in content
    provider = content["providers"][0]
    assert provider.get("allowUiUpdates") is False, "allowUiUpdates must be false"


def test_grafana_alerting_provisioning() -> None:
    alert_file = Path("infra/observability/grafana/provisioning/alerting/alerting.yml")
    assert alert_file.exists(), "alerting.yml must exist"

    with open(alert_file, encoding="utf-8") as f:
        content = yaml.safe_load(f)

    assert isinstance(content, dict)
    assert content.get("apiVersion") == 1
    assert "contactPoints" in content
    assert "policies" in content

    cp_names = [cp["name"] for cp in content["contactPoints"]]
    assert "AlertManager Webhook" in cp_names


def test_grafana_dashboard_json_validity() -> None:
    dashboards_dir = Path("infra/observability/grafana/dashboards")
    assert dashboards_dir.exists()

    json_files = list(dashboards_dir.glob("**/*.json"))
    assert len(json_files) >= 3, "At least 3 dashboard JSON files must exist"

    for dash_path in json_files:
        with open(dash_path, encoding="utf-8") as f:
            data = json.load(f)
        assert isinstance(data, dict)
        assert "title" in data
        assert "panels" in data
        assert len(data["panels"]) > 0
