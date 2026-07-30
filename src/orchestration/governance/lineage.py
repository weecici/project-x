"""OpenLineage and OpenMetadata lineage manifest extractor and compiler.

Dynamically extracts platform data flow nodes and directed graph edges by parsing:
1. System runtime configurations (BatchConfig, OlapLoaderConfig, StreamingConfig)
2. dbt AST compilation manifest (dbt/target/manifest.json)
3. Airflow DAGs, tasks, and asset outlets via Airflow DagBag API
"""

from __future__ import annotations

import glob
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from batch.config import BatchConfig
from olap.config import OlapLoaderConfig
from orchestration.config import GovernanceConfig
from streaming.config import StreamingConfig


@dataclass
class OpenLineageDataset:
    """OpenLineage v1.0 standard dataset facet entity."""

    namespace: str
    name: str
    facets: dict[str, Any] = field(default_factory=dict)


@dataclass
class OpenLineageJob:
    """OpenLineage v1.0 standard job entity."""

    namespace: str
    name: str
    facets: dict[str, Any] = field(default_factory=dict)


@dataclass
class OpenLineageRun:
    """OpenLineage v1.0 standard run execution instance."""

    runId: str
    facets: dict[str, Any] = field(default_factory=dict)


@dataclass
class OpenLineageRunEvent:
    """OpenLineage v1.0 standard RunEvent schema specification."""

    eventType: str  # START, RUNNING, COMPLETE, FAIL
    eventTime: str
    run: OpenLineageRun
    job: OpenLineageJob
    inputs: list[OpenLineageDataset]
    outputs: list[OpenLineageDataset]
    producer: str = "https://github.com/crypto-platform/lineage-compiler"
    schemaURL: str = (
        "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
    )


@dataclass
class EntityReference:
    """OpenMetadata standard EntityReference schema."""

    id: str
    type: str  # table, topic, pipeline, container, dashboard


@dataclass
class EntitiesEdge:
    """OpenMetadata standard EntitiesEdge relationship schema."""

    fromEntity: EntityReference
    toEntity: EntityReference


@dataclass
class LineageDetails:
    """OpenMetadata standard LineageDetails schema."""

    sqlQuery: str | None = None
    pipeline: EntityReference | None = None


@dataclass
class AddLineageRequest:
    """OpenMetadata standard AddLineageRequest REST API schema specification."""

    edge: EntitiesEdge
    lineageDetails: LineageDetails | None = None


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
    """Platform lineage manifest complying with OpenLineage & OpenMetadata specs."""

    version: str
    namespace: str
    nodes: list[LineageNode]
    edges: list[LineageEdge]
    openlineage_events: list[OpenLineageRunEvent] = field(default_factory=list)
    openmetadata_lineage_requests: list[AddLineageRequest] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest data hierarchy to dictionary for JSON serialization."""
        return {
            "version": self.version,
            "namespace": self.namespace,
            "openlineage_spec": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
            "openmetadata_spec": "https://open-metadata.org/schema/api/lineage/addLineageRequest.json",
            "openlineage_events": [asdict(ev) for ev in self.openlineage_events],
            "openmetadata_lineage_requests": [
                asdict(req) for req in self.openmetadata_lineage_requests
            ],
            "nodes": [asdict(node) for node in self.nodes],
            "edges": [asdict(edge) for edge in self.edges],
        }


def extract_runtime_config_nodes() -> tuple[list[LineageNode], list[LineageEdge]]:
    """Extract runtime nodes and edges dynamically from platform config objects."""
    batch_cfg = BatchConfig()
    olap_cfg = OlapLoaderConfig()
    streaming_cfg = StreamingConfig()

    nodes = [
        LineageNode(
            id="binance_ws",
            name="Binance WebSocket Stream",
            type="external_source",
            layer="ingestion",
            description="Live trade and kline WebSocket feeds from Binance API.",
        ),
        LineageNode(
            id=f"kafka_{streaming_cfg.kafka_topic_klines}",
            name=streaming_cfg.kafka_topic_klines,
            type="topic",
            layer="ingestion",
            description="Kafka KRaft topic holding raw kline JSON payloads.",
        ),
        LineageNode(
            id=f"minio_{batch_cfg.minio_bucket_bronze}",
            name=f"s3://{batch_cfg.minio_bucket_bronze}/klines/",
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
            id=f"minio_{batch_cfg.minio_bucket_silver}",
            name=f"s3://{batch_cfg.minio_bucket_silver}/klines/",
            type="bucket",
            layer="lake",
            description="Schema-enforced, partitioned Parquet silver lake dataset.",
        ),
        LineageNode(
            id=f"clickhouse_{olap_cfg.clickhouse_table_klines}",
            name=f"silver.{olap_cfg.clickhouse_table_klines}",
            type="table",
            layer="olap",
            description="ReplacingMergeTree table storing raw Arrow columnar records.",
        ),
    ]

    edges = [
        LineageEdge(
            from_node="binance_ws",
            to_node=f"kafka_{streaming_cfg.kafka_topic_klines}",
            relationship="produces",
        ),
        LineageEdge(
            from_node=f"kafka_{streaming_cfg.kafka_topic_klines}",
            to_node=f"minio_{batch_cfg.minio_bucket_bronze}",
            relationship="persists",
        ),
        LineageEdge(
            from_node=f"minio_{batch_cfg.minio_bucket_bronze}",
            to_node="pyspark_silver_job",
            relationship="transforms",
        ),
        LineageEdge(
            from_node="pyspark_silver_job",
            to_node=f"minio_{batch_cfg.minio_bucket_silver}",
            relationship="partition_writes",
        ),
        LineageEdge(
            from_node=f"minio_{batch_cfg.minio_bucket_silver}",
            to_node=f"clickhouse_{olap_cfg.clickhouse_table_klines}",
            relationship="arrow_loads",
        ),
    ]
    return nodes, edges


def extract_dbt_manifest_lineage(
    manifest_path: Path = Path("dbt/target/manifest.json"),
) -> tuple[list[LineageNode], list[LineageEdge]]:
    """Dynamically parse dbt compilation manifest JSON for models and dependencies."""
    nodes: list[LineageNode] = []
    edges: list[LineageEdge] = []

    if not manifest_path.exists():
        logger.warning("dbt manifest not found at {path}", path=manifest_path)
        return nodes, edges

    try:
        with open(manifest_path, encoding="utf-8") as f:
            data = json.load(f)

        dbt_nodes = data.get("nodes", {})
        dbt_sources = data.get("sources", {})

        # Extract dbt sources
        for key, source in dbt_sources.items():
            nodes.append(
                LineageNode(
                    id=key,
                    name=f"{source.get('schema')}.{source.get('name')}",
                    type="dbt_source",
                    layer="olap",
                    description=source.get("description", "dbt source table"),
                )
            )

        # Extract dbt models
        for key, node in dbt_nodes.items():
            if node.get("resource_type") == "model":
                node_id = key
                model_name = node.get("name")
                schema_name = node.get("schema", "gold")
                nodes.append(
                    LineageNode(
                        id=node_id,
                        name=f"{schema_name}.{model_name}",
                        type="table",
                        layer="olap",
                        description=node.get("description", "dbt SQL gold model"),
                    )
                )

                # Extract parent dependency edges dynamically from dbt AST
                depends_on = node.get("depends_on", {}).get("nodes", [])
                for parent_key in depends_on:
                    edges.append(
                        LineageEdge(
                            from_node=parent_key,
                            to_node=node_id,
                            relationship="dbt_model_dependency",
                        )
                    )

        logger.info(
            "Parsed dbt manifest dynamically | models={n_models} edges={n_edges}",
            n_models=len(nodes),
            n_edges=len(edges),
        )
    except Exception as exc:
        logger.warning("Failed parsing dbt manifest: {err}", err=str(exc))

    return nodes, edges


def extract_airflow_dag_lineage() -> tuple[list[LineageNode], list[LineageEdge]]:
    """Dynamically parse Airflow DagBag for DAG tasks, assets, and outlets."""
    nodes: list[LineageNode] = []
    edges: list[LineageEdge] = []

    try:
        from airflow.models import DagBag

        dagbag = DagBag(dag_folder="src/orchestration/dags")

        for dag_id, dag in dagbag.dags.items():
            dag_node_id = f"dag_{dag_id}"
            nodes.append(
                LineageNode(
                    id=dag_node_id,
                    name=dag_id,
                    type="airflow_dag",
                    layer="orchestration",
                    description=dag.description or f"Airflow DAG {dag_id}",
                )
            )

            for task in dag.tasks:
                task_node_id = f"task_{dag_id}_{task.task_id}"
                nodes.append(
                    LineageNode(
                        id=task_node_id,
                        name=f"{dag_id}.{task.task_id}",
                        type="airflow_task",
                        layer="orchestration",
                        description=f"Task operator: {task.__class__.__name__}",
                    )
                )
                edges.append(
                    LineageEdge(
                        from_node=dag_node_id,
                        to_node=task_node_id,
                        relationship="executes_task",
                    )
                )

                # Dynamically extract task outlets (Airflow Assets)
                for outlet in getattr(task, "outlets", []) or []:
                    uri = getattr(outlet, "uri", str(outlet))
                    asset_node_id = f"asset_{uri}"
                    nodes.append(
                        LineageNode(
                            id=asset_node_id,
                            name=uri,
                            type="asset",
                            layer="orchestration",
                            description="Airflow scheduled asset outlet",
                        )
                    )
                    edges.append(
                        LineageEdge(
                            from_node=task_node_id,
                            to_node=asset_node_id,
                            relationship="emits_asset",
                        )
                    )
    except Exception as exc:
        logger.warning("Failed parsing Airflow DAGs dynamically: {err}", err=str(exc))

    return nodes, edges


def extract_cube_semantic_lineage(
    cube_dir: Path = Path("cube/model"),
) -> tuple[list[LineageNode], list[LineageEdge]]:
    """Dynamically parse Cube.js YAML models for cubes, views, and dependencies."""
    nodes: list[LineageNode] = []
    edges: list[LineageEdge] = []

    if not cube_dir.exists():
        logger.warning("Cube model directory not found at {path}", path=cube_dir)
        return nodes, edges

    try:
        yaml_files = glob.glob(str(cube_dir / "**/*.yml"), recursive=True)

        for filepath in yaml_files:
            try:
                with open(filepath, encoding="utf-8") as f:
                    content = yaml.safe_load(f)
                if not content or not isinstance(content, dict):
                    continue

                # Parse Cubes
                for cube in content.get("cubes", []):
                    cube_name = cube.get("name")
                    sql_table = cube.get("sql_table")
                    if cube_name:
                        cube_node_id = f"cube_{cube_name}"
                        nodes.append(
                            LineageNode(
                                id=cube_node_id,
                                name=cube_name,
                                type="semantic_cube",
                                layer="semantic",
                                description=f"Cube model for table {sql_table}",
                            )
                        )
                        if sql_table:
                            edges.append(
                                LineageEdge(
                                    from_node=sql_table,
                                    to_node=cube_node_id,
                                    relationship="models_table",
                                )
                            )

                # Parse Views
                for view in content.get("views", []):
                    view_name = view.get("name")
                    if view_name:
                        view_node_id = f"view_{view_name}"
                        nodes.append(
                            LineageNode(
                                id=view_node_id,
                                name=view_name,
                                type="semantic_view",
                                layer="semantic",
                                description=view.get(
                                    "description", "Cube.js public view"
                                ),
                            )
                        )
                        for cube_ref in view.get("cubes", []):
                            parent_cube = cube_ref.get("join_path")
                            if parent_cube:
                                edges.append(
                                    LineageEdge(
                                        from_node=f"cube_{parent_cube}",
                                        to_node=view_node_id,
                                        relationship="exposes_cube",
                                    )
                                )
            except Exception as file_exc:
                logger.warning(
                    "Skipping Cube file {path}: {err}",
                    path=filepath,
                    err=str(file_exc),
                )

        logger.info(
            "Parsed Cube YAML schemas dynamically | nodes={n_nodes} edges={n_edges}",
            n_nodes=len(nodes),
            n_edges=len(edges),
        )
    except Exception as exc:
        logger.warning("Failed parsing Cube schemas: {err}", err=str(exc))

    return nodes, edges


def extract_bi_exporter_lineage() -> tuple[list[LineageNode], list[LineageEdge]]:
    """Dynamically parse BI exporter registry and configuration targets."""
    nodes: list[LineageNode] = []
    edges: list[LineageEdge] = []

    try:
        from olap.config import BiExporterConfig
        from olap.exporter import VIEWS_TO_EXPORT

        bi_config = BiExporterConfig()

        csv_export_id = "bi_csv_export"
        nodes.append(
            LineageNode(
                id=csv_export_id,
                name=f"CSV Exports ({bi_config.export_output_dir})",
                type="bi_export",
                layer="bi",
                description="Local CSV exports generated from Cube REST API",
            )
        )

        sheets_export_id = "bi_google_sheets_export"
        nodes.append(
            LineageNode(
                id=sheets_export_id,
                name=f"Google Sheets ({bi_config.google_sheet_name})",
                type="bi_export",
                layer="bi",
                description="Cloud Google Sheets sync for Tableau dashboards",
            )
        )

        for view_name in VIEWS_TO_EXPORT:
            view_node_id = f"view_{view_name}"
            edges.append(
                LineageEdge(
                    from_node=view_node_id,
                    to_node=csv_export_id,
                    relationship="exports_to_csv",
                )
            )
            edges.append(
                LineageEdge(
                    from_node=view_node_id,
                    to_node=sheets_export_id,
                    relationship="syncs_to_sheets",
                )
            )
    except Exception as exc:
        logger.warning("Failed parsing BI exporter config: {err}", err=str(exc))

    return nodes, edges


def build_platform_lineage_manifest(config: GovernanceConfig) -> LineageManifest:
    """Build the end-to-end lineage graph connecting ingestion through BI dynamically.

    Args:
        config: GovernanceConfig holding environment parameters.

    Returns:
        Populated LineageManifest graph object with dynamic nodes and edges.
    """
    nodes: list[LineageNode] = []
    edges: list[LineageEdge] = []

    # 1. Runtime Config Nodes & Edges
    cfg_nodes, cfg_edges = extract_runtime_config_nodes()
    nodes.extend(cfg_nodes)
    edges.extend(cfg_edges)

    # 2. dbt AST Manifest Dynamic Nodes & Edges
    dbt_nodes, dbt_edges = extract_dbt_manifest_lineage()
    nodes.extend(dbt_nodes)
    edges.extend(dbt_edges)

    # 3. Airflow DAGs & Assets Dynamic Nodes & Edges
    dag_nodes, dag_edges = extract_airflow_dag_lineage()
    nodes.extend(dag_nodes)
    edges.extend(dag_edges)

    # 4. Cube.js Semantic Schema YAML Dynamic Nodes & Edges
    cube_nodes, cube_edges = extract_cube_semantic_lineage()
    nodes.extend(cube_nodes)
    edges.extend(cube_edges)

    # 5. BI Exporter Config & Registry Dynamic Nodes & Edges
    bi_nodes, bi_edges = extract_bi_exporter_lineage()
    nodes.extend(bi_nodes)
    edges.extend(bi_edges)

    # Deduplicate nodes and edges by ID
    unique_nodes: dict[str, LineageNode] = {node.id: node for node in nodes}
    unique_edges_dict: dict[tuple[str, str, str], LineageEdge] = {
        (edge.from_node, edge.to_node, edge.relationship): edge for edge in edges
    }

    # Generate standard OpenLineage v1.0 RunEvents & OpenMetadata AddLineageRequests
    openlineage_events: list[OpenLineageRunEvent] = []
    openmetadata_requests: list[AddLineageRequest] = []

    for edge in unique_edges_dict.values():
        from_node = unique_nodes.get(edge.from_node)
        to_node = unique_nodes.get(edge.to_node)
        from_type = from_node.type if from_node else "table"
        to_type = to_node.type if to_node else "table"

        # 1. OpenMetadata standard AddLineageRequest payload
        om_request = AddLineageRequest(
            edge=EntitiesEdge(
                fromEntity=EntityReference(id=edge.from_node, type=from_type),
                toEntity=EntityReference(id=edge.to_node, type=to_type),
            ),
            lineageDetails=LineageDetails(
                sqlQuery=f"Relationship: {edge.relationship}"
            ),
        )
        openmetadata_requests.append(om_request)

        # 2. OpenLineage v1.0 standard RunEvent specification payload
        now_iso = datetime.now(UTC).isoformat()
        ol_event = OpenLineageRunEvent(
            eventType="COMPLETE",
            eventTime=now_iso,
            run=OpenLineageRun(runId=str(uuid.uuid4())),
            job=OpenLineageJob(
                namespace=config.openlineage_namespace,
                name=f"{edge.from_node}_to_{edge.to_node}",
            ),
            inputs=[
                OpenLineageDataset(
                    namespace=config.openlineage_namespace,
                    name=edge.from_node,
                )
            ],
            outputs=[
                OpenLineageDataset(
                    namespace=config.openlineage_namespace,
                    name=edge.to_node,
                )
            ],
        )
        openlineage_events.append(ol_event)

    return LineageManifest(
        version="1.13.1",
        namespace=config.openlineage_namespace,
        nodes=list(unique_nodes.values()),
        edges=list(unique_edges_dict.values()),
        openlineage_events=openlineage_events,
        openmetadata_lineage_requests=openmetadata_requests,
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
