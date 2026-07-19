"""Module to fetch data from Cube.js Semantic API and export/sync to targets.

Supports local CSV file generation and automated Google Sheets synchronization.
"""

from __future__ import annotations

import sys
from pathlib import Path

import gspread
import httpx
import pandas as pd
from loguru import logger

from olap.config import BiExporterConfig
from utils.logging import configure_logging

# Registry of semantic views and their dimension/measure columns
VIEWS_TO_EXPORT = {
    "ohlcv_daily": {
        "dimensions": ["ohlcv_daily.symbol", "ohlcv_daily.trade_date"],
        "measures": [
            "ohlcv_daily.total_volume",
            "ohlcv_daily.total_quote_volume",
            "ohlcv_daily.total_trades",
            "ohlcv_daily.avg_close",
            "ohlcv_daily.max_high",
            "ohlcv_daily.min_low",
        ],
    },
    "ohlcv_hourly": {
        "dimensions": ["ohlcv_hourly.symbol", "ohlcv_hourly.hour_at"],
        "measures": [
            "ohlcv_hourly.total_volume",
            "ohlcv_hourly.total_quote_volume",
            "ohlcv_hourly.total_trades",
            "ohlcv_hourly.avg_close",
            "ohlcv_hourly.max_high",
            "ohlcv_hourly.min_low",
        ],
    },
    "price_analytics": {
        "dimensions": [
            "price_analytics.symbol",
            "price_analytics.interval",
            "price_analytics.open_at",
        ],
        "measures": [
            "price_analytics.avg_log_return",
            "price_analytics.stddev_log_return",
        ],
    },
}


def sync_to_google_sheets(
    df: pd.DataFrame,
    sheet_name: str,
    worksheet_title: str,
    credential_path: str,
) -> None:
    """Sync a pandas DataFrame to a Google Sheet worksheet using a service account.

    If the worksheet does not exist inside the spreadsheet, it is created.

    Args:
        df: The pandas DataFrame holding normalized metrics.
        sheet_name: The name of the target Google Sheet spreadsheet.
        worksheet_title: The specific tab sheet name (e.g. 'ohlcv_daily').
        credential_path: Path to the service account JSON key file.
    """
    try:
        gc = gspread.service_account(filename=credential_path)
        sh = gc.open(sheet_name)

        try:
            worksheet = sh.worksheet(worksheet_title)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=worksheet_title, rows=1000, cols=20)
            logger.info("Created new worksheet: {title}", title=worksheet_title)

        # Convert datetime columns to strings for Sheets compatibility
        df_sync = df.copy()
        for col in df_sync.select_dtypes(include=["datetime", "datetimetz"]).columns:
            df_sync[col] = df_sync[col].astype(str)

        worksheet.clear()
        data = [df_sync.columns.values.tolist(), *df_sync.values.tolist()]
        worksheet.update(range_name="A1", values=data)
        logger.info(
            "Successfully synced {title} to Google Sheets", title=worksheet_title
        )
    except Exception as e:
        logger.error(
            "Failed to sync {title} to Google Sheets: {err}",
            title=worksheet_title,
            err=str(e),
        )


def export_semantic_data(config: BiExporterConfig) -> None:
    """Fetch metrics from Cube.js REST API and sync/write them to export targets.

    Args:
        config: The BiExporterConfig holding host and credential parameters.
    """
    # Ensure export directory exists
    config.export_output_dir.mkdir(parents=True, exist_ok=True)

    headers: dict[str, str] = {}

    with httpx.Client(timeout=30.0) as client:
        for view_name, query_body in VIEWS_TO_EXPORT.items():
            logger.info("Fetching view {view} from Cube REST API...", view=view_name)

            try:
                response = client.post(
                    f"{config.cube_api_url}/cubejs-api/v1/load",
                    json={"query": query_body},
                    headers=headers,
                )
                response.raise_for_status()
                payload = response.json()
            except Exception as e:
                logger.error(
                    "Failed to query Cube API for view {view}: {err}",
                    view=view_name,
                    err=str(e),
                )
                continue

            if "error" in payload:
                logger.error(
                    "Cube compilation error for view {view}: {err}",
                    view=view_name,
                    err=payload["error"],
                )
                continue

            records = payload.get("data", [])
            df = pd.DataFrame(records)

            if df.empty:
                logger.warning(
                    "No records returned from Cube for view {view}", view=view_name
                )
                expected_cols = query_body["dimensions"] + query_body["measures"]
                df = pd.DataFrame(columns=expected_cols)

            # Strip view prefix from column names
            # (e.g. 'ohlcv_daily.symbol' -> 'symbol')
            prefix = f"{view_name}."
            df.columns = [
                col.replace(prefix, "") if col.startswith(prefix) else col
                for col in df.columns
            ]

            # 1. Fallback save locally as CSV
            local_path = config.export_output_dir / f"fct_{view_name}.csv"
            df.to_csv(local_path, index=False, encoding="utf-8")
            logger.info(
                "Exported local file | rows={rows} path={path}",
                rows=len(df),
                path=local_path,
            )

            # 2. Sync to Google Sheets if credentials are configured
            if config.google_service_account_json:
                cred_path = Path(config.google_service_account_json)
                if cred_path.exists():
                    logger.info(
                        "Initiating Google Sheets sync for worksheet {ws}...",
                        ws=view_name,
                    )
                    sync_to_google_sheets(
                        df=df,
                        sheet_name=config.google_sheet_name,
                        worksheet_title=view_name,
                        credential_path=str(cred_path),
                    )
                else:
                    logger.warning(
                        "Google Service Account file not found at {path}. "
                        "Skipping cloud sync.",
                        path=cred_path,
                    )


def main() -> None:
    """Main CLI runner for BI exporter."""
    configure_logging()
    config = BiExporterConfig()
    logger.info(
        "BI exporter starting | cube_api_url={url} output_dir={dir}",
        url=config.cube_api_url,
        dir=config.export_output_dir,
    )
    export_semantic_data(config)
    logger.info("BI exporter complete.")


def cli() -> None:
    """CLI entrypoint (used by [project.scripts])."""
    try:
        main()
    except KeyboardInterrupt:
        logger.info("BI exporter stopped by user")
        sys.exit(0)


if __name__ == "__main__":
    cli()
