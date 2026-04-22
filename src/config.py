"""環境変数・地点設定の取得ロジック。"""

import json
import logging
import os

logger = logging.getLogger(__name__)


def get_env_str(key: str, default: str = "") -> str:
    return os.getenv(key, default)


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
