"""download_history.py のユニットテスト。"""

import unittest.mock as mock
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
import requests

from download_history import (
    _build_month_range,
    _fetch_all,
    _save_csv,
    download_5years_history,
)
from models import Location


def _make_location(**kwargs) -> Location:
    """テスト用 Location を生成するヘルパー。"""
    defaults = {
        "area_name": "首都圏",
        "prec_no": 44,
        "prec_name": "東京",
        "block_no": 47662,
        "block_name": "東京",
    }
    return Location(**{**defaults, **kwargs})


class TestBuildMonthRange:
    """_build_month_range のテスト。"""

    def test_returns_list_of_tuples(self):
        """(year, month) タプルのリストを返す。"""
        result = _build_month_range(1)
        assert isinstance(result, list)
        assert all(
            isinstance(item, tuple) and len(item) == 2 for item in result
        )

    def test_last_element_is_current_month(self):
        """最後の要素が現在の年月である。"""
        now = datetime.now()
        result = _build_month_range(1)
        assert result[-1] == (now.year, now.month)

    def test_elements_in_chronological_order(self):
        """要素が時系列順に並んでいる。"""
        result = _build_month_range(2)
        for i in range(len(result) - 1):
            assert result[i] < result[i + 1]

    def test_approximate_length_for_one_year(self):
        """1年分は12〜13件になる。"""
        result = _build_month_range(1)
        assert 12 <= len(result) <= 13

    def test_deterministic_with_mocked_now(self):
        """datetime.now をモックすると決定的な結果になる。"""
        with mock.patch("download_history.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2025, 3, 1)
            result = _build_month_range(1)
        assert result[0] == (2024, 3)
        assert result[-1] == (2025, 3)
        assert len(result) == 13


class TestFetchAll:
    """_fetch_all のテスト。"""

    def test_returns_empty_when_all_data_empty(self):
        """全地点のデータが空の場合は空リストを返す。"""
        loc = _make_location()
        with (
            mock.patch(
                "download_history.fetch_and_validate_weather"
            ) as mock_fetch,
            mock.patch("download_history.time.sleep"),
        ):
            mock_fetch.return_value = pd.DataFrame()
            result = _fetch_all([loc], [(2024, 1)])
        assert result == []

    def test_returns_dataframes_when_data_exists(self):
        """データが存在する場合は DataFrame のリストを返す。"""
        loc = _make_location()
        df = pd.DataFrame({"col": [1, 2]})
        with (
            mock.patch(
                "download_history.fetch_and_validate_weather"
            ) as mock_fetch,
            mock.patch("download_history.time.sleep"),
        ):
            mock_fetch.return_value = df
            result = _fetch_all([loc], [(2024, 1)])
        assert len(result) == 1

    def test_skips_on_http_error(self):
        """requests.HTTPError が発生した地点はスキップして処理を継続する。"""
        loc = _make_location()
        with (
            mock.patch(
                "download_history.fetch_and_validate_weather"
            ) as mock_fetch,
            mock.patch("download_history.time.sleep"),
        ):
            mock_fetch.side_effect = requests.HTTPError()
            result = _fetch_all([loc], [(2024, 1)])
        assert result == []

    def test_skips_on_value_error(self):
        """ValueError が発生した地点はスキップして処理を継続する。"""
        loc = _make_location()
        with (
            mock.patch(
                "download_history.fetch_and_validate_weather"
            ) as mock_fetch,
            mock.patch("download_history.time.sleep"),
        ):
            mock_fetch.side_effect = ValueError()
            result = _fetch_all([loc], [(2024, 1)])
        assert result == []

    def test_sleep_called_per_location_per_month(self):
        """地点 × 月ごとに sleep が呼ばれる。"""
        loc = _make_location()
        df = pd.DataFrame({"col": [1]})
        with (
            mock.patch(
                "download_history.fetch_and_validate_weather"
            ) as mock_fetch,
            mock.patch("download_history.time.sleep") as mock_sleep,
        ):
            mock_fetch.return_value = df
            _fetch_all([loc], [(2024, 1), (2024, 2)])
        assert mock_sleep.call_count == 2

    def test_fetch_called_for_each_combination(self):
        """地点 × 月の全組み合わせで fetch が呼ばれる。"""
        locs = [
            _make_location(block_name="東京"),
            _make_location(
                area_name="近畿",
                prec_no=62,
                prec_name="大阪",
                block_no=47772,
                block_name="大阪",
            ),
        ]
        df = pd.DataFrame({"col": [1]})
        with (
            mock.patch(
                "download_history.fetch_and_validate_weather"
            ) as mock_fetch,
            mock.patch("download_history.time.sleep"),
        ):
            mock_fetch.return_value = df
            _fetch_all(locs, [(2024, 1), (2024, 2)])
        assert mock_fetch.call_count == 4  # 2地点 × 2ヶ月


class TestSaveCsv:
    """_save_csv のテスト。"""

    def test_file_is_created(self, tmp_path: Path):
        """指定パスに CSV ファイルが作成される。"""
        df = pd.DataFrame({"col": [1, 2]})
        output = tmp_path / "test.csv"
        _save_csv(df, output)
        assert output.exists()

    def test_utf8_sig_encoding(self, tmp_path: Path):
        """ファイルが utf-8-sig（BOM 付き）で保存される。"""
        df = pd.DataFrame({"地点": ["東京"]})
        output = tmp_path / "test.csv"
        _save_csv(df, output)
        assert output.read_bytes()[:3] == b"\xef\xbb\xbf"

    def test_no_index_column(self, tmp_path: Path):
        """インデックス列が含まれない。"""
        df = pd.DataFrame({"col": [1]})
        output = tmp_path / "test.csv"
        _save_csv(df, output)
        content = output.read_text(encoding="utf-8-sig")
        assert "Unnamed" not in content


class TestDownload5YearsHistory:
    """download_5years_history のテスト。"""

    def test_saves_csv_when_data_exists(self, tmp_path: Path):
        """データが取得できた場合は CSV を保存する。"""
        output = tmp_path / "out.csv"
        df = pd.DataFrame({"col": [1]})
        with (
            mock.patch(
                "download_history._build_month_range"
            ) as mock_range,
            mock.patch("download_history._fetch_all") as mock_fetch,
            mock.patch("download_history.get_locations_from_env") as mock_locs,
        ):
            mock_range.return_value = [(2024, 1)]
            mock_fetch.return_value = [df]
            mock_locs.return_value = []
            download_5years_history(output_path=output)
        assert output.exists()

    def test_no_csv_when_no_data(self, tmp_path: Path):
        """データが1件も取得できなかった場合は CSV を保存しない。"""
        output = tmp_path / "out.csv"
        with (
            mock.patch(
                "download_history._build_month_range"
            ) as mock_range,
            mock.patch("download_history._fetch_all") as mock_fetch,
            mock.patch("download_history.get_locations_from_env") as mock_locs,
        ):
            mock_range.return_value = [(2024, 1)]
            mock_fetch.return_value = []
            mock_locs.return_value = []
            download_5years_history(output_path=output)
        assert not output.exists()

    def test_build_month_range_called_with_5_years(self, tmp_path: Path):
        """_build_month_range が years=5 で呼ばれる。"""
        output = tmp_path / "out.csv"
        with (
            mock.patch(
                "download_history._build_month_range"
            ) as mock_range,
            mock.patch("download_history._fetch_all") as mock_fetch,
            mock.patch("download_history.get_locations_from_env") as mock_locs,
        ):
            mock_range.return_value = [(2024, 1)]
            mock_fetch.return_value = []
            mock_locs.return_value = []
            download_5years_history(output_path=output)
        mock_range.assert_called_once_with(years=5)
