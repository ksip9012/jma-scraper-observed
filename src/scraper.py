"""気象庁からのデータ取得・パース・バリデーション処理。"""

import io
import logging
from datetime import date

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pydantic import ValidationError

from models import WeatherRecord

logger = logging.getLogger(__name__)

_META_KEYS = {
    "date",
    "area_name",
    "prec_no",
    "prec_name",
    "block_no",
    "block_name",
}


def _fetch_html(url: str) -> str:
    """気象庁のURLからHTMLを取得する。

    Args:
        url (str): 取得対象の URL。

    Returns:
        str: レスポンスの HTML テキスト。

    Raises:
        requests.HTTPError: HTTP エラーが発生した場合。
        requests.Timeout: リクエストがタイムアウトした場合。
    """
    response = requests.get(url, timeout=15)
    response.encoding = response.apparent_encoding
    response.raise_for_status()
    return response.text


def _parse_weather_table(html: str, year: int, month: int) -> pd.DataFrame:
    """HTMLから気象テーブルをパースし、フラットなカラムを持つDataFrameを返す。

    Args:
        html (str): 気象庁ページの HTML テキスト。
        year (int): 取得対象の年（エラーメッセージ用）。
        month (int): 取得対象の月（エラーメッセージ用）。

    Returns:
        pd.DataFrame: フラット化されたカラム名を持つ気象データの DataFrame。

    Raises:
        ValueError: HTML 内に気象テーブルが見つからない場合。
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", class_="data2_s")
    if not table:
        raise ValueError(f"{year}/{month} の気象テーブルが見つかりません。")

    dfs = pd.read_html(io.StringIO(str(table)), header=[0, 1, 2])
    df = dfs[0]

    if df.iloc[0, 0] == "日":
        df = df.iloc[1:].reset_index(drop=True)

    df.columns = [
        "_".join([str(c) for c in col if "Unnamed" not in str(c)])
        for col in df.columns
    ]
    return df


def _validate_weather_records(
    df: pd.DataFrame,
    prec_no: int,
    prec_name: str,
    block_no: int,
    block_name: str,
    area_name: str,
    year: int,
    month: int,
) -> list[dict]:
    """DataFrameの各行をPydanticでバリデーションし、有効なレコードのリストを返す。

    日付が空・不正な行や、観測値が1つも存在しない行（未来日付など）はスキップする。

    Args:
        df (pd.DataFrame): パース済みの気象データ DataFrame。
        prec_no (int): 都道府県番号。
        prec_name (str): 都道府県名。
        block_no (int): 地点番号。
        block_name (str): 地点名。
        area_name (str): エリア名。
        year (int): 取得対象の年。
        month (int): 取得対象の月。

    Returns:
        list[dict]: バリデーション済みの気象レコードの辞書リスト。
    """
    validated_records = []

    for record in df.to_dict(orient="records"):
        day_key = next(iter(record.keys()))
        day_val = record.get(day_key)

        if pd.isna(day_val) or str(day_val).strip() == "":
            continue

        try:
            day_int = int(float(day_val))
            current_date = date(year, month, day_int)
            record["date"] = current_date.isoformat()
            record["area_name"] = area_name
            record["prec_no"] = str(prec_no)
            record["prec_name"] = prec_name
            record["block_no"] = str(block_no)
            record["block_name"] = block_name

            v_record = WeatherRecord(**record)
            data_dict = v_record.model_dump()

            # 観測データが1つ以上存在するレコードのみ抽出
            # （気象庁の表にある未来の日付を除外するため）
            obs_values = [
                v for k, v in data_dict.items() if k not in _META_KEYS
            ]
            if any(v is not None for v in obs_values):
                validated_records.append(data_dict)

        except ValueError:
            continue
        except ValidationError:
            continue

    return validated_records


def fetch_and_validate_weather(
    prec_no: int,
    prec_name: str,
    block_no: int,
    block_name: str,
    area_name: str,
    year: int,
    month: int,
) -> pd.DataFrame:
    """気象庁からデータを取得し、Pydanticでバリデーションを行う。

    Args:
        prec_no (int): 都道府県番号。
        prec_name (str): 都道府県名。
        block_no (int): 地点番号。
        block_name (str): 地点名。
        area_name (str): エリア名。
        year (int): 取得対象の年。
        month (int): 取得対象の月。

    Returns:
        pd.DataFrame: 指定された月のバリデーション済み気象データ。
            有効なレコードが存在しない場合は空の DataFrame を返す。

    Raises:
        requests.HTTPError: 気象庁へのリクエストが失敗した場合。
        ValueError: HTML 内に気象テーブルが見つからない場合。
    """
    url = (
        f"https://www.data.jma.go.jp/stats/etrn/view/daily_s1.php?"
        f"prec_no={prec_no}&block_no={block_no}&"
        f"year={year}&month={month:02d}&day=&view=p1"
    )

    logger.info("%04d/%02d のデータを取得中: %s", year, month, url)
    html = _fetch_html(url)
    df = _parse_weather_table(html, year, month)
    validated_records = _validate_weather_records(
        df, prec_no, prec_name, block_no, block_name, area_name, year, month
    )

    if not validated_records:
        logger.warning(
            "%04d/%02d の有効な観測レコードが見つかりませんでした。",
            year,
            month,
        )
        return pd.DataFrame()

    return pd.DataFrame(validated_records)
