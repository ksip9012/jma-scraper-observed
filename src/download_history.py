"""過去5年分の気象データを一括で取得し、ローカルのCSVに保存するスクリプト。"""

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

from config import get_locations_from_env, setup_logging
from models import Location
from scraper import fetch_and_validate_weather

setup_logging()
logger = logging.getLogger(__name__)

_DEFAULT_OUTPUT = (
    Path(__file__).parent.parent / "data" / "weather_history_5y.csv"
)


def _build_month_range(years: int) -> list[tuple[int, int]]:
    """現在から遡って指定年数分の (year, month) リストを返す。

    Args:
        years (int): 遡る年数。

    Returns:
        list[tuple[int, int]]: 開始月から現在月までの (year, month) のリスト。
    """
    end_date = datetime.now()
    start_date = end_date - relativedelta(years=years)
    months = []
    current = start_date
    while current <= end_date:
        months.append((current.year, current.month))
        current += relativedelta(months=1)
    return months


def _fetch_all(
    locations: list[Location],
    month_range: list[tuple[int, int]],
) -> list[pd.DataFrame]:
    """地点 × 月の組み合わせでデータを取得し、DataFrame のリストを返す。

    気象庁サーバーへの負荷軽減のため、地点ごとに1秒の待機を挟む。

    Args:
        locations (list[Location]): 取得対象の観測地点リスト。
        month_range (list[tuple[int, int]]): 取得対象の (year, month) リスト。

    Returns:
        list[pd.DataFrame]: 取得に成功した DataFrame のリスト。
    """
    all_dfs = []
    for year, month in month_range:
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
    return all_dfs


def _save_csv(df: pd.DataFrame, output_path: Path) -> None:
    """DataFrame を CSV ファイルに保存する。

    utf-8-sig エンコードを使用することで Excel での文字化けを防ぐ。

    Args:
        df (pd.DataFrame): 保存対象の DataFrame。
        output_path (Path): 保存先のファイルパス。

    Returns:
        None
    """
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("-" * 30)
    logger.info("全ての処理が完了しました。")
    logger.info("保存先: %s", output_path)
    logger.info("合計レコード数: %d 行", len(df))


def download_5years_history(output_path: Path = _DEFAULT_OUTPUT) -> None:
    """現在から遡って5年分のデータを全地点分取得し、CSVに保存する。

    取得対象地点は環境変数 JMA_LOCATIONS または
    デフォルト地点リストから取得する。

    Args:
        output_path (Path): 保存先のファイルパス。
            デフォルトは `../data/weather_history_5y.csv`。

    Returns:
        None
    """
    locations = get_locations_from_env()
    month_range = _build_month_range(years=5)

    logger.info(
        "%04d/%02d から %04d/%02d までのデータを取得します。（%d地点）",
        month_range[0][0],
        month_range[0][1],
        month_range[-1][0],
        month_range[-1][1],
        len(locations),
    )

    all_dfs = _fetch_all(locations, month_range)

    if all_dfs:
        master_df = pd.concat(all_dfs, ignore_index=True)
        _save_csv(master_df, output_path)
    else:
        logger.error("データが1件も取得できませんでした。")


if __name__ == "__main__":
    download_5years_history()
