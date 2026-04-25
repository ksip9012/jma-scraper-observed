"""main.py の weather_ingestion_handler のユニットテスト。"""

import unittest.mock as mock

import pandas as pd
import pytest

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
        "block_name": ["東京", "東京"],
    }
)


class TestWeatherIngestionHandler:
    """weather_ingestion_handler のテスト。"""

    def test_returns_500_when_no_project_id(self, monkeypatch):
        """GCP_PROJECT_ID 未設定時はステータス 500 を返す。"""
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        _, status = weather_ingestion_handler()
        assert status == 500

    def test_returns_200_with_valid_data(self, monkeypatch):
        """正常系（データあり）はステータス 200 を返す。"""
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
            _, status = weather_ingestion_handler()
        assert status == 200

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

    def test_returns_200_when_no_data(self, monkeypatch):
        """全地点でデータなし時はステータス 200 を返す。"""
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
            _, status = weather_ingestion_handler()
        assert status == 200
        mock_upload.assert_not_called()
