"""気象庁データを取得し、BigQuery へアップロードする Cloud Run Jobs 処理。"""

import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery

from bigquery_client import delete_month_data, upload_to_bigquery
from config import get_locations_from_env
from scraper import fetch_and_validate_weather

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def weather_ingestion_handler() -> None:
    """気象データの取得・バリデーション・BigQuery アップロードを実行する。

    Returns:
        None

    Raises:
        RuntimeError: GCP_PROJECT_ID が未設定の場合。
        google.cloud.exceptions.GoogleCloudError: BigQuery 操作が失敗した場合。
    """
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID", "weather_data")
    table_id = os.getenv("BQ_TABLE_ID", "daily_stats")

    if not project_id:
        raise RuntimeError("GCP_PROJECT_ID が設定されていません。")

    locations = get_locations_from_env()
    target_date = datetime.now() - timedelta(days=1)
    year, month = target_date.year, target_date.month

    client = bigquery.Client(project=project_id)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    delete_month_data(client, table_full_id, year, month)

    all_dfs = []
    for loc in locations:
        try:
            df = fetch_and_validate_weather(
                location=loc,
                year=year,
                month=month,
            )
            if not df.empty:
                all_dfs.append(df)
        except Exception:
            logger.exception("%s のデータ取得に失敗しました。", loc.block_name)

    if not all_dfs:
        logger.warning(
            "%04d/%02d のアップロード対象データがありません。", year, month
        )
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)
    upload_to_bigquery(
        combined_df, project_id, dataset_id, table_id, table_full_id
    )
    logger.info(
        "%s を %04d/%02d のデータで更新しました。", table_full_id, year, month
    )


if __name__ == "__main__":
    try:
        weather_ingestion_handler()
    except Exception:
        logger.exception("処理が失敗しました。")
        sys.exit(1)
