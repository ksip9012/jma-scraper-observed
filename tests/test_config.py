"""config.py のユニットテスト。"""

import pytest

from config import (
    get_default_locations,
    get_locations_from_env,
    parse_location_json,
)
from models import Location


class TestParseLocationJson:
    """parse_location_json のテスト。"""

    def test_valid_json_returns_locations(self):
        """正常な JSON 文字列から Location リストを返す。"""
        json_str = (
            '[{"area_name": "首都圏", "prec_no": 44,'
            ' "prec_name": "東京", "block_no": 47662,'
            ' "block_name": "東京"}]'
        )
        result = parse_location_json(json_str)
        assert len(result) == 1
        assert isinstance(result[0], Location)
        assert result[0].block_name == "東京"

    def test_multiple_locations(self):
        """複数地点の JSON を正しくパースできる。"""
        json_str = (
            '[{"area_name": "首都圏", "prec_no": 44,'
            ' "prec_name": "東京", "block_no": 47662,'
            ' "block_name": "東京"},'
            ' {"area_name": "近畿", "prec_no": 62,'
            ' "prec_name": "大阪", "block_no": 47772,'
            ' "block_name": "大阪"}]'
        )
        result = parse_location_json(json_str)
        assert len(result) == 2
        assert result[1].block_name == "大阪"

    def test_invalid_json_returns_empty_list(self):
        """不正な JSON 文字列は空リストを返す。"""
        result = parse_location_json("not a json")
        assert result == []

    def test_missing_required_field_returns_empty_list(self):
        """必須フィールドが欠けた JSON は空リストを返す。"""
        json_str = '[{"area_name": "首都圏"}]'
        result = parse_location_json(json_str)
        assert result == []

    def test_empty_list_json_returns_empty_list(self):
        """空配列 JSON は空リストを返す。"""
        result = parse_location_json("[]")
        assert result == []


class TestGetDefaultLocations:
    """get_default_locations のテスト。"""

    def test_returns_list_of_locations(self):
        """Location のリストを返す。"""
        result = get_default_locations()
        assert isinstance(result, list)
        assert all(isinstance(loc, Location) for loc in result)

    def test_returns_six_locations(self):
        """デフォルトは 6 地点を返す。"""
        result = get_default_locations()
        assert len(result) == 6

    def test_contains_tokyo(self):
        """東京が含まれている。"""
        result = get_default_locations()
        block_names = [loc.block_name for loc in result]
        assert "東京" in block_names

    def test_all_have_required_fields(self):
        """全地点が必須フィールドを持つ。"""
        for loc in get_default_locations():
            assert loc.area_name
            assert loc.prec_no > 0
            assert loc.block_no > 0


class TestGetLocationsFromEnv:
    """get_locations_from_env のテスト。"""

    def test_returns_default_when_env_not_set(self, monkeypatch):
        """JMA_LOCATIONS 未設定時はデフォルト地点を返す。"""
        monkeypatch.delenv("JMA_LOCATIONS", raising=False)
        result = get_locations_from_env()
        assert len(result) == 6

    def test_returns_env_locations_when_set(self, monkeypatch):
        """JMA_LOCATIONS が正常に設定されている場合はその値を返す。"""
        json_str = (
            '[{"area_name": "首都圏", "prec_no": 44,'
            ' "prec_name": "東京", "block_no": 47662,'
            ' "block_name": "東京"}]'
        )
        monkeypatch.setenv("JMA_LOCATIONS", json_str)
        result = get_locations_from_env()
        assert len(result) == 1
        assert result[0].block_name == "東京"

    def test_falls_back_to_default_on_invalid_json(self, monkeypatch):
        """JMA_LOCATIONS が不正な JSON の場合はデフォルト地点を返す。"""
        monkeypatch.setenv("JMA_LOCATIONS", "invalid json")
        result = get_locations_from_env()
        assert len(result) == 6
