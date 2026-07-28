"""OpenLineage and OpenMetadata lineage manifest extractor and compiler.

Extracts platform node definitions and directed graph edges, compiling an
OpenMetadata-compliant lineage JSON manifest.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger

from orchestration.config import GovernanceConfig


@dataclass
class LineageNode:
    """Represents a data entity or transformation node in the lineage graph."""

    id: str
    name: str
    type: str  # e.g., 'topic', 'bucket', 'spark_job', 'table', 'semantic_view'
    layer: str  # e.g., 'ingestion', 'lake', 'olap', 'semantic', 'bi'
    description: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEdge:
    """Represents a directed data flow edge between two lineage nodes."""

    from_node: str
    to_node: str
    relationship: str  # e.g., 'produces', 'transforms', 'loads', 'exposes'


@dataclass
class LineageManifest:
    """Complete platform data lineage graph manifest."""

    version: str
    namespace: str
    nodes: list[LineageNode]
    edges: list[LineageEdge]

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest data hierarchy to dictionary for JSON serialization."""
        return {
            "version": self.version,
            "namespace": self.namespace,
            "nodes": [asdict(node) for node in self.nodes],
            "edges": [asdict(edge) for edge in self.edges],
        }


def build_platform_lineage_manifest(config: GovernanceConfig) -> LineageManifest:
    """Build the end-to-end lineage graph connecting ingestion through BI.

    Args:
        config: GovernanceConfig holding environment parameters.

    Returns:
        Populated LineageManifest graph object.
    """
    nodes = [
        # 1. Ingestion Layer
        LineageNode(
            id="binance_ws",
            name="Binance WebSocket Stream",
            type="external_source",
            layer="ingestion",
            description="Live trade and kline WebSocket feeds from Binance API.",
        ),
        LineageNode(
            id="kafka_raw_klines",
            name="raw.klines",
            type="topic",
            layer="ingestion",
            description="Kafka KRaft topic holding raw kline JSON payloads.",
        ),
        # 2. Lakehouse Layer
        LineageNode(
            id="minio_bronze",
            name="s3://bronze/klines/",
            type="bucket",
            layer="lake",
            description="Raw append-only JSON/Parquet dumps in MinIO.",
        ),
        LineageNode(
            id="pyspark_silver_job",
            name="Spark Silver Transformer",
            type="spark_job",
            layer="lake",
            description="PySpark transformation: deduplication & Hive partitioning.",
        ),
        LineageNode(
            id="minio_silver",
            name="s3://silver/klines/",
            type="bucket",
            layer="lake",
            description="Schema-enforced, partitioned Parquet silver lake dataset.",
        ),
        # 3. OLAP Layer
        LineageNode(
            id="clickhouse_raw_klines",
            name="silver.klines_raw",
            type="table",
            layer="olap",
            description="ReplacingMergeTree table storing raw Arrow columnar records.",
        ),
        LineageNode(
            id="dbt_gold_daily",
            name="gold.fct_daily_klines",
            type="table",
            layer="olap",
            description="dbt transformed gold mart for daily aggregated OHLCV metrics.",
        ),
        LineageNode(
            id="dbt_gold_returns",
            name="gold.fct_kline_returns",
            type="table",
            layer="olap",
            description="dbt gold mart for log returns and volatility analytics.",
        ),
        # 4. Semantic & BI Layer
        LineageNode(
            id="cube_ohlcv_daily",
            name="ohlcv_daily",
            type="semantic_view",
            layer="semantic",
            description="Cube.js public view exposing metrics via REST/SQL API.",
        ),
        LineageNode(
            id="tableau_public_sheets",
            name="Google Sheets BI / Tableau Public",
            type="bi_export",
            layer="bi",
            description="Automated Google Sheets cloud sync for Tableau dashboards.",
        ),
    ]

    edges = [
        LineageEdge(
            from_node="binance_ws", to_node="kafka_raw_klines", relationship="produces"
        ),
        LineageEdge(
            from_node="kafka_raw_klines",
            to_node="minio_bronze",
            relationship="persists",
        ),
        LineageEdge(
            from_node="minio_bronze",
            to_node="pyspark_silver_job",
            relationship="transforms",
        ),
        LineageEdge(
            from_node="pyspark_silver_job",
            to_node="minio_silver",
            relationship="partition_writes",
        ),
        LineageEdge(
            from_node="minio_silver",
            to_node="clickhouse_raw_klines",
            relationship="arrow_loads",
        ),
        LineageEdge(
            from_node="clickhouse_raw_klines",
            to_node="dbt_gold_daily",
            relationship="dbt_models",
        ),
        LineageEdge(
            from_node="clickhouse_raw_klines",
            to_node="dbt_gold_returns",
            relationship="dbt_models",
        ),
        LineageEdge(
            from_node="dbt_gold_daily",
            to_node="cube_ohlcv_daily",
            relationship="exposes",
        ),
        LineageEdge(
            from_node="cube_ohlcv_daily",
            to_node="tableau_public_sheets",
            relationship="exports",
        ),
    ]

    return LineageManifest(
        version="1.13.1",
        namespace=config.openlineage_namespace,
        nodes=nodes,
        edges=edges,
    )


def export_lineage_manifest(config: GovernanceConfig) -> Path:
    """Compile and export the platform lineage manifest to a JSON artifact file.

    Args:
        config: GovernanceConfig specifying output directory.

    Returns:
        Path to generated JSON lineage manifest file.
    """
    out_dir = config.lineage_output_dir
    manifest = build_platform_lineage_manifest(config)
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = out_dir / "lineage_manifest.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(manifest.to_dict(), f, indent=2)
    except PermissionError:
        logger.warning(
            "Permission denied writing to {path}; falling back to /tmp/exports",
            path=out_dir,
        )
        out_dir = Path("/tmp/exports")
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = out_dir / "lineage_manifest.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(manifest.to_dict(), f, indent=2)

    logger.info(
        "Exported lineage manifest | nodes={n_nodes} edges={n_edges} path={path}",
        n_nodes=len(manifest.nodes),
        n_edges=len(manifest.edges),
        path=output_path,
    )
    return output_path
