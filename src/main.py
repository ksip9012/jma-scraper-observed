"""気象庁の気象データを取得し、BigQueryにアップロードするCloud Function。

実行時の日付から自動的に対象の年月を特定し、気象庁からデータを取得します。
Pydantic でバリデーションを行い、BigQuery のテーブルを更新します。
"""

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery

from bigquery_client import delete_month_data, upload_to_bigquery
from config import get_locations_from_env
from scraper import fetch_and_validate_weather

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def weather_ingestion_handler(request: object = None) -> tuple[str, int]:
    """Cloud Functionのエントリポイント（HTTP/PubSubトリガー）。

    Args:
        request (object, optional): Cloud Functionのリクエストオブジェクト。

    Returns:
        tuple[str, int]: レスポンスメッセージとHTTPステータスコード。
    """
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID", "weather_data")
    table_id = os.getenv("BQ_TABLE_ID", "daily_stats")

    if not project_id:
        logger.error("GCP_PROJECT_ID が設定されていません。")
        return "Error: GCP_PROJECT_ID not set", 500

    locations = get_locations_from_env()
    target_date = datetime.now() - timedelta(days=1)
    year, month = target_date.year, target_date.month

    client = bigquery.Client(project=project_id)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    try:
        delete_month_data(client, table_full_id, year, month)
    except Exception as e:
        logger.exception("既存データの削除に失敗しました。")
        return f"Error: {str(e)}", 500

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
        return f"{year}/{month} のアップロード対象データがありません", 200

    combined_df = pd.concat(all_dfs, ignore_index=True)

    try:
        upload_to_bigquery(
            combined_df, project_id, dataset_id, table_id, table_full_id
        )
    except Exception as e:
        logger.exception("BigQuery へのアップロードに失敗しました。")
        return f"Error: {str(e)}", 500

    return f"Successfully updated {table_full_id} for {year}/{month}", 200


if __name__ == "__main__":
    print(weather_ingestion_handler())
