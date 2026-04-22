"""BigQuery へのデータ削除・アップロード処理。"""

import logging

import pandas as pd
import pandas_gbq
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def delete_month_data(
    client: bigquery.Client,
    table_full_id: str,
    year: int,
    month: int,
) -> None:
    """指定した年月の既存データを BigQuery から削除する。"""
    date_prefix = f"{year}-{month:02d}-"
    delete_query = f"""
        DELETE FROM `{table_full_id}`
        WHERE STARTS_WITH(date, '{date_prefix}')
    """
    logger.info(
        "%04d/%02d の既存データを削除中: %s", year, month, table_full_id
    )
    client.query(delete_query).result()


def upload_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    table_full_id: str,
) -> None:
    """DataFrame を BigQuery テーブルへ追記アップロードする。"""
    logger.info(
        "%d 行のデータを %s へアップロード中 (追記)...",
        len(df),
        table_full_id,
    )
    pandas_gbq.to_gbq(
        df,
        f"{dataset_id}.{table_id}",
        project_id=project_id,
        if_exists="append",
        progress_bar=False,
    )
