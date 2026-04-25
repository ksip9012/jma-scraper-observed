"""models.py の WeatherRecord / Location モデルのユニットテスト。"""

import pytest
from pydantic import ValidationError

from models import Location, WeatherRecord


class TestCleanJmaSymbols:
    """WeatherRecord.clean_jma_symbols バリデーターのテスト。

    temperature_mean は alias（気温(℃)_気温(℃)_平均）経由でのみ
    モデルに渡せるため、バリデーターをクラスメソッドとして直接呼び出す。
    """

    def test_none_returns_none(self):
        """None はそのまま None を返す。"""
        assert WeatherRecord.clean_jma_symbols(None) is None

    def test_nan_returns_none(self):
        """float NaN は None を返す。"""
        import math

        assert WeatherRecord.clean_jma_symbols(math.nan) is None

    @pytest.mark.parametrize("symbol", ["--", "×", "///", ""])
    def test_jma_missing_symbols_return_none(self, symbol):
        """気象庁の欠損記号は None を返す。"""
        assert WeatherRecord.clean_jma_symbols(symbol) is None

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("12.3)", "12.3"),
            ("5.6]", "5.6"),
            ("7.8*", "7.8"),
            ("9.0)*", "9.0"),
        ],
    )
    def test_trailing_symbols_are_stripped(self, raw, expected):
        """末尾の )、]、* は除去される。"""
        assert WeatherRecord.clean_jma_symbols(raw) == expected

    def test_normal_value_passes_through(self):
        """通常の数値文字列はそのまま保持される。"""
        assert WeatherRecord.clean_jma_symbols("15.2") == "15.2"


class TestWeatherRecordRequired:
    """WeatherRecord の必須フィールドのテスト。"""

    def test_missing_required_field_raises(self):
        """必須フィールドが欠けている場合は ValidationError が発生する。"""
        with pytest.raises(ValidationError):
            WeatherRecord(
                area_name="首都圏",
                prec_no="44",
                prec_name="東京",
                block_no="47662",
                block_name="東京",
            )

    def test_all_optional_fields_default_to_none(self):
        """省略可能フィールドはデフォルトで None になる。"""
        r = WeatherRecord(
            date="2024-01-01",
            area_name="首都圏",
            prec_no="44",
            prec_name="東京",
            block_no="47662",
            block_name="東京",
        )
        assert r.temperature_mean is None
        assert r.precipitation_total is None
        assert r.wind_speed_mean is None


class TestLocation:
    """Location モデルのテスト。"""

    def test_valid_location(self):
        """正常な値で Location が作成できる。"""
        loc = Location(
            area_name="首都圏",
            prec_no=44,
            prec_name="東京",
            block_no=47662,
            block_name="東京",
        )
        assert loc.area_name == "首都圏"
        assert loc.prec_no == 44
        assert loc.block_no == 47662

    def test_model_validate_from_dict(self):
        """dict から model_validate で Location が作成できる。"""
        data = {
            "area_name": "近畿",
            "prec_no": 62,
            "prec_name": "大阪",
            "block_no": 47772,
            "block_name": "大阪",
        }
        loc = Location.model_validate(data)
        assert loc.prec_name == "大阪"

    def test_missing_field_raises(self):
        """必須フィールドが欠けている場合は ValidationError が発生する。"""
        with pytest.raises(ValidationError):
            Location(
                area_name="首都圏",
                prec_no=44,
                prec_name="東京",
                block_no=47662,
            )

    def test_invalid_type_raises(self):
        """prec_no に文字列を渡した場合、数値変換できなければ ValidationError。"""
        with pytest.raises(ValidationError):
            Location(
                area_name="首都圏",
                prec_no="invalid",
                prec_name="東京",
                block_no=47662,
                block_name="東京",
            )
