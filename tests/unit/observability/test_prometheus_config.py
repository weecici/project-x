"""Unit tests for Prometheus scrape configuration and alerting rules."""

from pathlib import Path

import yaml


def test_prometheus_yml_validity() -> None:
    prom_file = Path("infra/observability/prometheus/prometheus.yml")
    assert prom_file.exists(), "prometheus.yml must exist"

    with open(prom_file, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    assert isinstance(config, dict)
    assert "scrape_configs" in config
    assert len(config["scrape_configs"]) >= 6

    job_names = [j["job_name"] for j in config["scrape_configs"]]
    expected_jobs = [
        "prometheus",
        "node-exporter",
        "cadvisor",
        "kafka-exporter",
        "clickhouse",
        "airflow-statsd",
        "minio",
    ]
    for expected in expected_jobs:
        assert expected in job_names, f"Job {expected} missing from prometheus.yml"


def test_prometheus_rules_validity() -> None:
    rules_dir = Path("infra/observability/prometheus/rules")
    assert rules_dir.exists(), "Prometheus rules directory must exist"

    rule_files = list(rules_dir.glob("*.yml"))
    assert len(rule_files) >= 3

    for rule_file in rule_files:
        with open(rule_file, encoding="utf-8") as f:
            content = yaml.safe_load(f)
        assert isinstance(content, dict)
        assert "groups" in content
        assert len(content["groups"]) > 0
        for group in content["groups"]:
            assert "name" in group
            assert "rules" in group
            for rule in group["rules"]:
                assert "expr" in rule
                assert ("alert" in rule and "for" in rule) or ("record" in rule)
