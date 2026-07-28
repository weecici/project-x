"""CLI runner for governance lineage manifest compilation."""

from __future__ import annotations

import sys

from loguru import logger

from orchestration.config import GovernanceConfig
from orchestration.governance.lineage import export_lineage_manifest
from utils.logging import configure_logging


def main() -> None:
    """Run lineage manifest exporter."""
    configure_logging()
    config = GovernanceConfig()
    logger.info(
        "Lineage compiler starting | namespace={ns} output_dir={dir}",
        ns=config.openlineage_namespace,
        dir=config.lineage_output_dir,
    )
    output_file = export_lineage_manifest(config)
    logger.info("Lineage export complete | file={file}", file=output_file)


def cli() -> None:
    """CLI entrypoint (registered in pyproject.toml)."""
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Lineage export cancelled by user")
        sys.exit(0)


if __name__ == "__main__":
    cli()
