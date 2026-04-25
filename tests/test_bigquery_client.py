"""bigquery_client.py のユニットテスト。"""

import unittest.mock as mock

import pandas as pd
import pytest

from bigquery_client import delete_month_data, upload_to_bigquery


class TestDeleteMonthData:
    """delete_month_data のテスト。"""

    def test_query_is_called(self):
        """client.query が1回呼ばれる。"""
        mock_client = mock.MagicMock()
        delete_month_data(mock_client, "proj.ds.tbl", 2024, 1)
        mock_client.query.assert_called_once()

    def test_result_is_called(self):
        """query().result() が呼ばれる。"""
        mock_client = mock.MagicMock()
        delete_month_data(mock_client, "proj.ds.tbl", 2024, 1)
        mock_client.query.return_value.result.assert_called_once()

    def test_query_contains_date_prefix(self):
        """クエリに正しい年月プレフィックスが含まれる。"""
        mock_client = mock.MagicMock()
        delete_month_data(mock_client, "proj.ds.tbl", 2024, 1)
        query = mock_client.query.call_args[0][0]
        assert "2024-01-" in query

    def test_query_contains_table_id(self):
        """クエリに対象テーブル ID が含まれる。"""
        mock_client = mock.MagicMock()
        delete_month_data(mock_client, "proj.ds.tbl", 2024, 1)
        query = mock_client.query.call_args[0][0]
        assert "proj.ds.tbl" in query

    @pytest.mark.parametrize(
        "month, expected_prefix",
        [(1, "2024-01-"), (12, "2024-12-")],
    )
    def test_month_zero_padded(self, month, expected_prefix):
        """月は2桁ゼロ埋めされる。"""
        mock_client = mock.MagicMock()
        delete_month_data(mock_client, "proj.ds.tbl", 2024, month)
        query = mock_client.query.call_args[0][0]
        assert expected_prefix in query


class TestUploadToBigquery:
    """upload_to_bigquery のテスト。"""

    def test_to_gbq_is_called(self):
        """pandas_gbq.to_gbq が1回呼ばれる。"""
        df = pd.DataFrame({"col": [1, 2]})
        with mock.patch("bigquery_client.pandas_gbq.to_gbq") as mock_to_gbq:
            upload_to_bigquery(df, "proj", "ds", "tbl", "proj.ds.tbl")
        mock_to_gbq.assert_called_once()

    def test_correct_project_id(self):
        """正しい project_id で呼ばれる。"""
        df = pd.DataFrame({"col": [1]})
        with mock.patch("bigquery_client.pandas_gbq.to_gbq") as mock_to_gbq:
            upload_to_bigquery(df, "my-project", "ds", "tbl", "my-project.ds.tbl")
        assert mock_to_gbq.call_args.kwargs["project_id"] == "my-project"

    def test_append_mode(self):
        """if_exists="append" で呼ばれる。"""
        df = pd.DataFrame({"col": [1]})
        with mock.patch("bigquery_client.pandas_gbq.to_gbq") as mock_to_gbq:
            upload_to_bigquery(df, "proj", "ds", "tbl", "proj.ds.tbl")
        assert mock_to_gbq.call_args.kwargs["if_exists"] == "append"

    def test_progress_bar_disabled(self):
        """progress_bar=False で呼ばれる。"""
        df = pd.DataFrame({"col": [1]})
        with mock.patch("bigquery_client.pandas_gbq.to_gbq") as mock_to_gbq:
            upload_to_bigquery(df, "proj", "ds", "tbl", "proj.ds.tbl")
        assert mock_to_gbq.call_args.kwargs["progress_bar"] is False
