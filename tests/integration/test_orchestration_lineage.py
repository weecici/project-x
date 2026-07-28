"""Integration tests for governance lineage manifest export."""

from __future__ import annotations

import json
from pathlib import Path

from orchestration.config import GovernanceConfig
from orchestration.governance.lineage import export_lineage_manifest


def test_export_lineage_manifest(tmp_path: Path) -> None:
    """Verify that export_lineage_manifest creates a valid JSON artifact."""
    config = GovernanceConfig(
        _env_file=None,  # type: ignore[call-arg]
        lineage_output_dir=tmp_path,
    )

    output_path = export_lineage_manifest(config)

    assert output_path.exists()
    assert output_path.name == "lineage_manifest.json"

    with open(output_path, encoding="utf-8") as f:
        data = json.load(f)

    assert data["version"] == "1.13.1"
    assert data["namespace"] == "crypto-platform"
    assert len(data["nodes"]) >= 8
    assert len(data["edges"]) >= 8
