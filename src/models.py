"""気象庁の気象データの Pydantic モデル定義。"""

import math
from typing import Any

from pydantic import BaseModel, Field, field_validator


class Location(BaseModel):
    """気象庁の観測地点を定義するモデル。"""

    area_name: str
    prec_no: int
    prec_name: str
    block_no: int
    block_name: str


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
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None

        str_val = str(value)
        clean_val = str_val.replace(")", "").replace("]", "").replace("*", "")

        if clean_val in ["--", "×", "///", ""]:
            return None

        return clean_val
