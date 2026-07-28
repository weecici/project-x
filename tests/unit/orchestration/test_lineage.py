"""Unit tests for lineage graph manifest builder."""

from __future__ import annotations

from orchestration.config import GovernanceConfig
from orchestration.governance.lineage import build_platform_lineage_manifest


class TestLineageManifestBuilder:
    """Test data structure and completeness of platform lineage manifest."""

    def test_manifest_nodes_and_edges(self) -> None:
        """Verify build_platform_lineage_manifest generates valid nodes/edges."""
        config = GovernanceConfig(_env_file=None)  # type: ignore[call-arg]
        manifest = build_platform_lineage_manifest(config)

        assert manifest.version == "1.13.1"
        assert manifest.namespace == "crypto-platform"
        assert len(manifest.nodes) >= 8
        assert len(manifest.edges) >= 8

        # Check layers represented
        layers = {node.layer for node in manifest.nodes}
        assert "ingestion" in layers
        assert "lake" in layers
        assert "olap" in layers
        assert "semantic" in layers
        assert "bi" in layers

    def test_manifest_dict_serialization(self) -> None:
        """Verify serialization to dictionary for JSON output."""
        config = GovernanceConfig(_env_file=None)  # type: ignore[call-arg]
        manifest = build_platform_lineage_manifest(config)
        data = manifest.to_dict()

        assert "version" in data
        assert "nodes" in data
        assert "edges" in data
        assert isinstance(data["nodes"], list)
        assert isinstance(data["edges"], list)
