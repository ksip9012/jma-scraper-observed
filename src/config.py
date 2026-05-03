"""環境変数・地点設定の取得ロジック。"""

import json
import logging
import os
import tomllib
from pathlib import Path

from pydantic import ValidationError

from models import Location

_LOCATIONS_TOML = Path(__file__).parent / "locations.toml"

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """ロギングの初期設定を行う。

    エントリポイント（main.py, download_history.py）から1回だけ呼び出す。
    各モジュールは logging.getLogger(__name__) のみ使用し、
    この関数は呼び出さない。

    Returns:
        None
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )


def get_env_str(key: str, default: str = "") -> str:
    """環境変数を文字列として取得する。

    Args:
        key (str): 環境変数名。
        default (str): 環境変数が未設定の場合のデフォルト値。

    Returns:
        str: 環境変数の値。未設定の場合は default を返す。
    """
    return os.getenv(key, default)


def parse_location_json(locations_json: str) -> list[Location]:
    """JSON 文字列を Location モデルのリストにパースする。

    Args:
        locations_json (str): 地点設定を表す JSON 文字列。

    Returns:
        list[Location]: Location モデルのリスト。
            パースまたはバリデーションに失敗した場合は空リストを返す。
    """
    try:
        raw_list = json.loads(locations_json)
        return [Location.model_validate(item) for item in raw_list]
    except (json.JSONDecodeError, ValidationError) as e:
        logger.error("JMA_LOCATIONSのパースに失敗: %s", e)
        return []


def get_default_locations() -> list[Location]:
    """locations.toml からデフォルトの観測地点リストを読み込んで返す。

    Returns:
        list[Location]: デフォルトの観測地点の Location モデルリスト。
    """
    with _LOCATIONS_TOML.open("rb") as f:
        data = tomllib.load(f)
    return [Location.model_validate(item) for item in data["locations"]]


def get_locations_from_env() -> list[Location]:
    """環境変数またはデフォルト値から観測地点リストを取得する。

    環境変数 JMA_LOCATIONS に有効な JSON が設定されている場合はそれを使用し、
    未設定または不正な場合はデフォルト地点リストを返す。

    Returns:
        list[Location]: 観測地点の Location モデルリスト。
    """
    locations_json = get_env_str("JMA_LOCATIONS")
    if locations_json:
        locations = parse_location_json(locations_json)
        if locations:
            return locations
    return get_default_locations()
