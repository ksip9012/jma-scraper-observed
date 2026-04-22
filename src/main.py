"""気象庁の気象データを取得し、BigQueryにアップロードするCloud Function。

実行時の日付から自動的に対象の年月を特定し、気象庁からデータを取得します。
Pydantic（Python 3.10+ の型ヒントを使用）でバリデーションを行い、
BigQueryのテーブルを最新のデータで置換します。
"""

import io
import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import pandas_gbq
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from pydantic import BaseModel, Field, ValidationError, field_validator

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherRecord(BaseModel):
    """気象庁の1日あたりの気象データを定義するスキーマ。"""

    date: str
    area_name: str
    prec_no: str
    prec_name: str
    block_no: str
    block_name: str

    # 気圧
    pressure_station_mean: str | None = Field(
        None, alias="気圧(hPa)_現地_平均"
    )
    pressure_sea_level_mean: str | None = Field(
        None, alias="気圧(hPa)_海面_平均"
    )

    # 降水量
    precipitation_total: str | None = Field(
        None, alias="降水量(mm)_降水量(mm)_合計"
    )
    precipitation_max_1h: str | None = Field(
        None, alias="降水量(mm)_降水量(mm)_最大"
    )
    precipitation_max_10min: str | None = Field(
        None, alias="降水量(mm)_降水量(mm)_最大.1"
    )

    # 気温
    temperature_mean: str | None = Field(None, alias="気温(℃)_気温(℃)_平均")
    temperature_max: str | None = Field(None, alias="気温(℃)_気温(℃)_最高")
    temperature_min: str | None = Field(None, alias="気温(℃)_気温(℃)_最低")

    # 湿度
    humidity_mean: str | None = Field(None, alias="湿度(％)_湿度(％)_平均")
    humidity_min: str | None = Field(None, alias="湿度(％)_湿度(％)_最小")

    # 風向・風速
    wind_speed_mean: str | None = Field(
        None, alias="風向・風速(m/s)_風向・風速(m/s)_平均 風速"
    )
    wind_speed_max: str | None = Field(
        None, alias="風向・風速(m/s)_風向・風速(m/s)_最大風速"
    )
    wind_direction_max: str | None = Field(
        None, alias="風向・風速(m/s)_風向・風速(m/s)_最大風速.1"
    )
    wind_speed_peak_gust: str | None = Field(
        None, alias="風向・風速(m/s)_風向・風速(m/s)_最大瞬間風速"
    )
    wind_direction_peak_gust: str | None = Field(
        None, alias="風向・風速(m/s)_風向・風速(m/s)_最大瞬間風速.1"
    )

    # その他
    sunshine_duration: str | None = Field(
        None, alias="日照 時間 (h)_日照 時間 (h)_日照 時間 (h)"
    )
    snowfall_total: str | None = Field(None, alias="雪(cm)_雪(cm)_降雪")
    snow_depth_max: str | None = Field(None, alias="雪(cm)_雪(cm)_最深積雪")

    # 天気概況
    weather_daytime: str | None = Field(
        None, alias="天気概況_天気概況_昼 (06:00-18:00)"
    )
    weather_nighttime: str | None = Field(
        None, alias="天気概況_天気概況_夜 (18:00-翌日06:00)"
    )

    @field_validator("*", mode="before")
    @classmethod
    def clean_jma_symbols(cls, value: Any) -> Any:
        """バリデーション前に気象庁特有の記号をクリーニングする。

        Args:
            value (Any): 気象庁の表から取得した生のデータ。

        Returns:
            Any: クリーニング後の文字列。欠損値の場合は None を返す。
        """
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None

        # 数値などの型も一旦文字列に変換して処理
        str_val = str(value)

        # ')', ']', '*' などの記号を削除
        clean_val = str_val.replace(")", "").replace("]", "").replace("*", "")

        if clean_val in ["--", "×", "///", ""]:
            return None

        return clean_val


def get_env_str(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def get_env_int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


def parse_location_json(locations_json: str) -> list[dict]:
    try:
        return json.loads(locations_json)
    except Exception as e:
        logger.error(f"JMA_LOCATIONSのパースに失敗: {e}")
        return []


def get_default_locations() -> list[dict]:
    return [
        {
            "area_name": "首都圏",
            "prec_no": 44,
            "prec_name": "東京",
            "block_no": 47662,
            "block_name": "東京",
        },
        {
            "area_name": "近畿",
            "prec_no": 62,
            "prec_name": "大阪",
            "block_no": 47772,
            "block_name": "大阪",
        },
        {
            "area_name": "九州",
            "prec_no": 82,
            "prec_name": "福岡",
            "block_no": 47807,
            "block_name": "福岡",
        },
        {
            "area_name": "四国",
            "prec_no": 72,
            "prec_name": "香川",
            "block_no": 47891,
            "block_name": "高松",
        },
        {
            "area_name": "中国",
            "prec_no": 67,
            "prec_name": "広島",
            "block_no": 47765,
            "block_name": "広島",
        },
        {
            "area_name": "東海",
            "prec_no": 51,
            "prec_name": "愛知",
            "block_no": 47636,
            "block_name": "名古屋",
        },
    ]


def get_locations_from_env() -> list[dict]:
    locations_json = get_env_str("JMA_LOCATIONS")
    if locations_json:
        locations = parse_location_json(locations_json)
        if locations:
            return locations
    return get_default_locations()


_META_KEYS = {
    "date",
    "area_name",
    "prec_no",
    "prec_name",
    "block_no",
    "block_name",
}


def _fetch_html(url: str) -> str:
    """気象庁のURLからHTMLを取得する。"""
    response = requests.get(url, timeout=15)
    response.encoding = response.apparent_encoding
    response.raise_for_status()
    return response.text


def _parse_weather_table(html: str, year: int, month: int) -> pd.DataFrame:
    """HTMLから気象テーブルをパースし、フラットなカラムを持つDataFrameを返す。"""
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
        for col in df.columns.values
    ]
    return df


def _validate_weather_records(
    df: pd.DataFrame,
    prec_no: int,
    prec_name: str,
    block_no: int,
    block_name: str,
    year: int,
    month: int,
) -> list[dict]:
    """DataFrameの各行をPydanticでバリデーションし、有効なレコードのリストを返す。"""
    validated_records = []

    for record in df.to_dict(orient="records"):
        day_key = list(record.keys())[0]
        day_val = record.get(day_key)

        if pd.isna(day_val) or str(day_val).strip() == "":
            continue

        try:
            day_int = int(float(day_val))
            current_date = date(year, month, day_int)
            record["date"] = current_date.isoformat()
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

        except ValueError, ValidationError:
            continue

    return validated_records


def fetch_and_validate_weather(
    prec_no: int,
    prec_name: str,
    block_no: int,
    block_name: str,
    year: int,
    month: int,
) -> pd.DataFrame:
    """気象庁からデータを取得し、Pydanticでバリデーションを行う。

    Args:
        prec_no (int): 都道府県番号。
        prec_name (str): 都道府県名。
        block_no (int): 地点番号。
        block_name (str): 地点名。
        year (int): 取得対象の年。
        month (int): 取得対象の月。

    Returns:
        pd.DataFrame: 指定された月のバリデーション済み気象データ。
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
        df, prec_no, prec_name, block_no, block_name, year, month
    )

    if not validated_records:
        logger.warning(
            "%04d/%02d の有効な観測レコードが見つかりませんでした。",
            year,
            month,
        )
        return pd.DataFrame()

    return pd.DataFrame(validated_records)


def weather_ingestion_handler(request: Any = None) -> tuple[str, int]:
    """Cloud Functionのエントリポイント（HTTP/PubSubトリガー）。

    Args:
        request (Any, optional): Cloud Functionのリクエストオブジェクト。
            デフォルトは None。

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

    # 対象年月の特定: 実行時の前日を使用
    target_date = datetime.now() - timedelta(days=1)
    year, month = target_date.year, target_date.month

    # BigQuery クライアント初期化
    client = bigquery.Client(project=project_id)
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"

    # 既存の当月分データを削除 (冪等性を担保)
    date_prefix = f"{year}-{month:02d}-"
    delete_query = f"""
        DELETE FROM `{table_full_id}`
        WHERE STARTS_WITH(date, '{date_prefix}')
    """
    logger.info(
        "%04d/%02d の既存データを削除中: %s", year, month, table_full_id
    )

    try:
        client.query(delete_query).result()
    except Exception as e:
        logger.exception("既存データの削除に失敗しました。")
        return f"Error: {str(e)}", 500

    all_dfs = []
    for loc in locations:
        try:
            df = fetch_and_validate_weather(
                loc["prec_no"],
                loc["prec_name"],
                loc["block_no"],
                loc["block_name"],
                year,
                month,
            )
            if not df.empty:
                all_dfs.append(df)
        except Exception:
            logger.exception(
                "%s のデータ取得に失敗しました。", loc.get("block_name")
            )

    if not all_dfs:
        return f"{year}/{month} のアップロード対象データがありません", 200

    combined_df = pd.concat(all_dfs, ignore_index=True)

    try:
        logger.info(
            "%d 行のデータを %s へアップロード中 (追記)...",
            len(combined_df),
            table_full_id,
        )
        pandas_gbq.to_gbq(
            combined_df,
            f"{dataset_id}.{table_id}",
            project_id=project_id,
            if_exists="append",
            progress_bar=False,
        )
    except Exception as e:
        logger.exception("BigQuery へのアップロードに失敗しました。")
        return f"Error: {str(e)}", 500

    return f"Successfully updated {table_full_id} for {year}/{month}", 200


if __name__ == "__main__":
    # ローカル実行テスト用
    print(weather_ingestion_handler())
