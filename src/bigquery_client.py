"""BigQuery へのデータ削除・アップロード処理。"""

import logging

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def delete_month_data(
    client: bigquery.Client,
    table_full_id: str,
    year: int,
    month: int,
) -> None:
    """指定した年月の既存データを BigQuery から削除する。

    冪等性を担保するため、アップロード前に当月分のデータを削除する。

    Args:
        client (bigquery.Client): BigQuery クライアント。
        table_full_id (str): 対象テーブルのフル ID
            （例: `project.dataset.table`）。
        year (int): 削除対象の年。
        month (int): 削除対象の月。

    Raises:
        google.cloud.exceptions.GoogleCloudError: BigQuery へのクエリが
            失敗した場合。
    """
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
    client: bigquery.Client,
    df: pd.DataFrame,
    table_full_id: str,
) -> None:
    """DataFrame を BigQuery テーブルへ追記アップロードする。

    Args:
        client (bigquery.Client): BigQuery クライアント。
        df (pd.DataFrame): アップロード対象のデータフレーム。
        table_full_id (str): 対象テーブルのフル ID
            （例: `project.dataset.table`）。

    Raises:
        google.cloud.exceptions.GoogleCloudError: アップロードが
            失敗した場合。
    """
    logger.info(
        "%d 行のデータを %s へアップロード中 (追記)...",
        len(df),
        table_full_id,
    )
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    client.load_table_from_dataframe(
        df, table_full_id, job_config=job_config
    ).result()
