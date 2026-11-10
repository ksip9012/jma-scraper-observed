"""main.py の weather_ingestion_handler のユニットテスト。"""

import unittest.mock as mock
from datetime import datetime

import pandas as pd
import pytest
import requests

from config import JST
from main import weather_ingestion_handler
from models import Location

# テスト用の観測地点（単一地点に限定してテストを単純化）
SINGLE_LOCATION = [
    Location(
        area_name="首都圏",
        prec_no=44,
        prec_name="東京",
        block_no=47662,
        block_name="東京",
    )
]

# 有効なレコードを含む DataFrame フィクスチャ
SAMPLE_DF = pd.DataFrame(
    {
        "date": ["2024-01-01", "2024-01-02"],
        "block_no": ["47662", "47662"],
        "block_name": ["東京", "東京"],
    }
)

TWO_LOCATIONS = [
    *SINGLE_LOCATION,
    Location(
        area_name="首都圏",
        prec_no=45,
        prec_name="千葉",
        block_no=47682,
        block_name="千葉",
    ),
]


class TestWeatherIngestionHandler:
    """weather_ingestion_handler のテスト。"""

    def test_raises_when_no_project_id(self, monkeypatch):
        """GCP_PROJECT_ID 未設定時は RuntimeError を送出する。"""
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        with pytest.raises(RuntimeError, match="GCP_PROJECT_ID"):
            weather_ingestion_handler()

    def test_succeeds_with_valid_data(self, monkeypatch):
        """正常系（データあり）は例外を送出せずに完了する。"""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        with (
            mock.patch("main.bigquery.Client"),
            mock.patch("main.delete_month_data"),
            mock.patch(
                "main.get_locations_from_env", return_value=SINGLE_LOCATION
            ),
            mock.patch(
                "main.fetch_and_validate_weather", return_value=SAMPLE_DF
            ),
            mock.patch("main.upload_to_bigquery"),
        ):
            weather_ingestion_handler()  # 例外が出なければ成功

    def test_upload_is_called_with_data(self, monkeypatch):
        """データあり時は upload_to_bigquery が呼ばれる。"""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        with (
            mock.patch("main.bigquery.Client"),
            mock.patch("main.delete_month_data"),
            mock.patch(
                "main.get_locations_from_env", return_value=SINGLE_LOCATION
            ),
            mock.patch(
                "main.fetch_and_validate_weather", return_value=SAMPLE_DF
            ),
            mock.patch("main.upload_to_bigquery") as mock_upload,
        ):
            weather_ingestion_handler()
        mock_upload.assert_called_once()

    def test_succeeds_when_no_data(self, monkeypatch):
        """全地点でデータなし時は例外を送出せず、upload は呼ばれない。"""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        with (
            mock.patch("main.bigquery.Client"),
            mock.patch("main.delete_month_data"),
            mock.patch(
                "main.get_locations_from_env", return_value=SINGLE_LOCATION
            ),
            mock.patch(
                "main.fetch_and_validate_weather",
                return_value=pd.DataFrame(),
            ),
            mock.patch("main.upload_to_bigquery") as mock_upload,
        ):
            weather_ingestion_handler()  # 例外が出なければ成功
        mock_upload.assert_not_called()

    def test_target_month_uses_yesterday_in_japan_time(self, monkeypatch):
        """対象の月は、日本時間の昨日で決める。"""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        with (
            mock.patch("main.datetime") as mock_datetime,
            mock.patch("main.bigquery.Client"),
            mock.patch(
                "main.get_locations_from_env", return_value=SINGLE_LOCATION
            ),
            mock.patch(
                "main.fetch_and_validate_weather",
                return_value=pd.DataFrame(),
            ) as mock_fetch,
        ):
            mock_datetime.now.return_value = datetime(
                2026, 3, 2, 0, 30, tzinfo=JST
            )
            weather_ingestion_handler()
        mock_datetime.now.assert_called_once_with(JST)
        assert mock_fetch.call_args.kwargs["year"] == 2026
        assert mock_fetch.call_args.kwargs["month"] == 3
||||||| b3c695f


    @pytest.mark.parametrize(
        "error",
        [
            requests.HTTPError("500"),
            requests.Timeout("timeout"),
            requests.ConnectionError("connection"),
            ValueError("parse"),
        ],
    )
    def test_nothing_is_deleted_when_all_fetches_fail(
        self, monkeypatch, error
    ):
        """全地点の取得に失敗したら、既存データを削除しない。"""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        with (
            mock.patch("main.bigquery.Client"),
            mock.patch("main.delete_month_data") as mock_delete,
            mock.patch(
                "main.get_locations_from_env", return_value=TWO_LOCATIONS
            ),
            mock.patch("main.fetch_and_validate_weather", side_effect=error),
            mock.patch("main.upload_to_bigquery") as mock_upload,
        ):
            weather_ingestion_handler()
        mock_delete.assert_not_called()
        mock_upload.assert_not_called()

    def test_deletes_only_locations_fetched_successfully(self, monkeypatch):
        """一部の地点が失敗したら、取得できた地点だけを入れ替える。"""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        with (
            mock.patch("main.bigquery.Client"),
            mock.patch("main.delete_month_data") as mock_delete,
            mock.patch(
                "main.get_locations_from_env", return_value=TWO_LOCATIONS
            ),
            mock.patch(
                "main.fetch_and_validate_weather",
                side_effect=[SAMPLE_DF, requests.Timeout("timeout")],
            ),
            mock.patch("main.upload_to_bigquery") as mock_upload,
        ):
            weather_ingestion_handler()
        mock_delete.assert_called_once()
        assert mock_delete.call_args.args[-1] == ["47662"]
        mock_upload.assert_called_once()

    def test_deletes_after_fetching(self, monkeypatch):
        """既存データの削除は、全地点の取得が終わってから行う。"""
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        calls = mock.MagicMock()
        with (
            mock.patch("main.bigquery.Client"),
            mock.patch("main.delete_month_data", calls.delete),
            mock.patch(
                "main.get_locations_from_env", return_value=SINGLE_LOCATION
            ),
            mock.patch(
                "main.fetch_and_validate_weather",
                calls.fetch,
            ),
            mock.patch("main.upload_to_bigquery", calls.upload),
        ):
            calls.fetch.return_value = SAMPLE_DF
            weather_ingestion_handler()
        assert [c[0] for c in calls.mock_calls] == [
            "fetch",
            "delete",
            "upload",
        ]
