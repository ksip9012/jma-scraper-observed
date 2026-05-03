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

    def test_load_table_is_called(self):
        """client.load_table_from_dataframe が1回呼ばれる。"""
        df = pd.DataFrame({"col": [1, 2]})
        mock_client = mock.MagicMock()
        upload_to_bigquery(mock_client, df, "proj.ds.tbl")
        mock_client.load_table_from_dataframe.assert_called_once()

    def test_correct_table_full_id(self):
        """正しい table_full_id で呼ばれる。"""
        df = pd.DataFrame({"col": [1]})
        mock_client = mock.MagicMock()
        upload_to_bigquery(mock_client, df, "my-project.ds.tbl")
        args = mock_client.load_table_from_dataframe.call_args
        assert args.args[1] == "my-project.ds.tbl"

    def test_append_mode(self):
        """WRITE_APPEND の write_disposition で呼ばれる。"""
        df = pd.DataFrame({"col": [1]})
        mock_client = mock.MagicMock()
        upload_to_bigquery(mock_client, df, "proj.ds.tbl")
        job_config = mock_client.load_table_from_dataframe.call_args.kwargs[
            "job_config"
        ]
        assert job_config.write_disposition == "WRITE_APPEND"

    def test_result_is_called(self):
        """load_table_from_dataframe().result() が呼ばれる。"""
        df = pd.DataFrame({"col": [1]})
        mock_client = mock.MagicMock()
        upload_to_bigquery(mock_client, df, "proj.ds.tbl")
        mock_client.load_table_from_dataframe.return_value.result.assert_called_once()
