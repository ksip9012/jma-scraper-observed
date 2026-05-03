"""過去5年分の気象データを一括で取得し、ローカルのCSVに保存するスクリプト。"""

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

from config import get_locations_from_env
from scraper import fetch_and_validate_weather

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def download_5years_history() -> None:
    """現在から遡って5年分のデータを全地点分取得し、1つのCSVに保存する。

    取得対象地点は環境変数 JMA_LOCATIONS または
    デフォルト地点リストから取得する。
    出力先は `../data/weather_history_5y.csv`（utf-8-sig エンコード）。
    気象庁サーバーへの負荷軽減のため、地点ごとに1秒の待機を挟む。

    Returns:
        None
    """
    locations = get_locations_from_env()

    end_date = datetime.now()
    start_date = end_date - relativedelta(years=5)

    logger.info(
        "%s から %s までのデータを取得します。（%d地点）",
        start_date.strftime("%Y/%m"),
        end_date.strftime("%Y/%m"),
        len(locations),
    )

    all_dfs = []
    current_date = start_date

    while current_date <= end_date:
        year, month = current_date.year, current_date.month

        for loc in locations:
            try:
                df = fetch_and_validate_weather(
                    location=loc,
                    year=year,
                    month=month,
                )

                if not df.empty:
                    all_dfs.append(df)
                    logger.info(
                        "%04d/%02d %s: %d件のデータを取得完了",
                        year,
                        month,
                        loc.block_name,
                        len(df),
                    )
                else:
                    logger.warning(
                        "%04d/%02d %s: データがありませんでした",
                        year,
                        month,
                        loc.block_name,
                    )

                time.sleep(1)

            except (requests.HTTPError, ValueError) as e:
                logger.error(
                    "%04d/%02d %s の取得中にエラーが発生しました: %s",
                    year,
                    month,
                    loc.block_name,
                    e,
                )

        current_date += relativedelta(months=1)

    if all_dfs:
        master_df = pd.concat(all_dfs, ignore_index=True)
        output_file = (
            Path(__file__).parent.parent / "data" / "weather_history_5y.csv"
        )
        # utf-8-sig を使うとExcelで開いた際の文字化けを防げます
        master_df.to_csv(output_file, index=False, encoding="utf-8-sig")

        logger.info("-" * 30)
        logger.info("全ての処理が完了しました。")
        logger.info("保存先: %s", output_file)
        logger.info("合計レコード数: %d 行", len(master_df))
    else:
        logger.error("データが1件も取得できませんでした。")


if __name__ == "__main__":
    download_5years_history()
