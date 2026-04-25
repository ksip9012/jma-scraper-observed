"""scraper.py のユニットテスト。"""

import unittest.mock as mock

import pandas as pd
import pytest
import requests
import responses as responses_mock

from models import Location
from scraper import (
    _fetch_html,
    _parse_weather_table,
    _validate_weather_records,
    fetch_and_validate_weather,
)

# テスト用の観測地点
TOKYO = Location(
    area_name="首都圏",
    prec_no=44,
    prec_name="東京",
    block_no=47662,
    block_name="東京",
)

# 最小限の気象テーブル HTML フィクスチャ
# パース後のカラム名: ["日_日_日", "気温(℃)_気温(℃)_平均"]
MINIMAL_TABLE_HTML = """<html><body>
<table class="data2_s">
  <tr><th rowspan="3">日</th><th>気温(℃)</th></tr>
  <tr><th>気温(℃)</th></tr>
  <tr><th>平均</th></tr>
  <tr><td>1</td><td>15.2</td></tr>
  <tr><td>2</td><td>16.0</td></tr>
</table>
</body></html>"""

# 先頭データ行が「日」の HTML フィクスチャ（1行スキップされる）
TABLE_HTML_WITH_HI_ROW = """<html><body>
<table class="data2_s">
  <tr><th rowspan="3">日</th><th>気温(℃)</th></tr>
  <tr><th>気温(℃)</th></tr>
  <tr><th>平均</th></tr>
  <tr><td>日</td><td>気温(℃)_気温(℃)_平均</td></tr>
  <tr><td>1</td><td>15.2</td></tr>
</table>
</body></html>"""


class TestFetchHtml:
    """_fetch_html のテスト。"""

    @responses_mock.activate
    def test_success_returns_html(self):
        """正常なレスポンスは HTML テキストを返す。"""
        responses_mock.add(
            responses_mock.GET,
            "http://example.com/test",
            body="<html>OK</html>",
            status=200,
        )
        result = _fetch_html("http://example.com/test")
        assert "<html>" in result

    @responses_mock.activate
    def test_http_error_raises(self):
        """HTTP 404 は HTTPError を送出する。"""
        responses_mock.add(
            responses_mock.GET,
            "http://example.com/test",
            status=404,
        )
        with pytest.raises(requests.HTTPError):
            _fetch_html("http://example.com/test")

    @responses_mock.activate
    def test_timeout_raises(self):
        """タイムアウト時は Timeout を送出する。"""
        responses_mock.add(
            responses_mock.GET,
            "http://example.com/test",
            body=requests.Timeout(),
        )
        with pytest.raises(requests.Timeout):
            _fetch_html("http://example.com/test")


class TestParseWeatherTable:
    """_parse_weather_table のテスト。"""

    def test_valid_html_returns_dataframe(self):
        """正常な HTML から DataFrame を返す。"""
        df = _parse_weather_table(MINIMAL_TABLE_HTML, 2024, 1)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_columns_are_flattened(self):
        """マルチレベルのカラムがフラット化される。"""
        df = _parse_weather_table(MINIMAL_TABLE_HTML, 2024, 1)
        assert "気温(℃)_気温(℃)_平均" in df.columns

    def test_no_table_raises_value_error(self):
        """気象テーブルが存在しない場合は ValueError を送出する。"""
        html = "<html><body><p>テーブルなし</p></body></html>"
        with pytest.raises(ValueError, match="気象テーブル"):
            _parse_weather_table(html, 2024, 1)

    def test_first_row_hi_is_skipped(self):
        """先頭データ行が「日」の場合はスキップされる。"""
        df = _parse_weather_table(TABLE_HTML_WITH_HI_ROW, 2024, 1)
        assert len(df) == 1
        assert df.iloc[0, 0] != "日"


class TestValidateWeatherRecords:
    """_validate_weather_records のテスト。"""

    def _make_df(
        self,
        days: list,
        temp_values: list,
    ) -> pd.DataFrame:
        """テスト用 DataFrame を作成するヘルパー。"""
        return pd.DataFrame(
            {
                "日": days,
                "気温(℃)_気温(℃)_平均": temp_values,
            }
        )

    def test_valid_records_returned(self):
        """正常な行はバリデーション済みレコードとして返される。"""
        df = self._make_df([1.0, 2.0], ["15.2", "16.0"])
        records = _validate_weather_records(df, TOKYO, 2024, 1)
        assert len(records) == 2
        assert records[0]["temperature_mean"] == "15.2"

    def test_metadata_is_set_correctly(self):
        """date・area_name 等のメタデータが正しくセットされる。"""
        df = self._make_df([5.0], ["12.5"])
        records = _validate_weather_records(df, TOKYO, 2024, 3)
        assert records[0]["date"] == "2024-03-05"
        assert records[0]["area_name"] == "首都圏"
        assert records[0]["prec_name"] == "東京"
        assert records[0]["block_name"] == "東京"

    def test_nan_day_is_skipped(self):
        """日付列が NaN の行はスキップされる。"""
        df = self._make_df([float("nan"), 1.0], [None, "15.2"])
        records = _validate_weather_records(df, TOKYO, 2024, 1)
        assert len(records) == 1

    def test_invalid_day_string_is_skipped(self):
        """日付列が数値変換不可の文字列の行はスキップされる。"""
        df = self._make_df(["abc", 1.0], ["15.2", "16.0"])
        records = _validate_weather_records(df, TOKYO, 2024, 1)
        assert len(records) == 1

    def test_out_of_range_day_is_skipped(self):
        """存在しない日付（32日など）の行はスキップされる。"""
        df = self._make_df([32.0, 1.0], ["15.2", "16.0"])
        records = _validate_weather_records(df, TOKYO, 2024, 1)
        assert len(records) == 1

    def test_all_obs_none_is_skipped(self):
        """観測値がすべて None の行はスキップされる（未来日付など）。"""
        df = self._make_df([15.0], [None])
        records = _validate_weather_records(df, TOKYO, 2024, 6)
        assert len(records) == 0


class TestFetchAndValidateWeather:
    """fetch_and_validate_weather のテスト。"""

    def test_success_returns_dataframe(self):
        """正常系は有効レコードを含む DataFrame を返す。"""
        with mock.patch("scraper._fetch_html", return_value=MINIMAL_TABLE_HTML):
            result = fetch_and_validate_weather(TOKYO, 2024, 1)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result) == 2

    def test_empty_dataframe_when_no_valid_records(self):
        """有効なレコードがない場合は空の DataFrame を返す。"""
        with (
            mock.patch(
                "scraper._fetch_html", return_value=MINIMAL_TABLE_HTML
            ),
            mock.patch(
                "scraper._validate_weather_records", return_value=[]
            ),
        ):
            result = fetch_and_validate_weather(TOKYO, 2024, 1)
        assert isinstance(result, pd.DataFrame)
        assert result.empty
