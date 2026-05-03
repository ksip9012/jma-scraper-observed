# jma-scraper-observed

気象庁の観測データを取得し、BigQuery へアップロードする Python プロジェクト。

## 概要

- 気象庁の公開 Web サイトから日別の気象観測データをスクレイピングして取得する
- 取得したデータを Google Cloud BigQuery へアップロードする（Cloud Run Jobs で実行）
- 過去5年分のデータを一括取得する CLI スクリプトも同梱している

## 必要な環境変数

| 変数名 | 必須 | 説明 |
|---|---|---|
| `GCP_PROJECT_ID` | ✓ | Google Cloud プロジェクト ID |
| `BQ_DATASET_ID` | | BigQuery データセット ID（デフォルト: `weather_data`） |
| `BQ_TABLE_ID` | | BigQuery テーブル ID（デフォルト: `daily_stats`） |
| `JMA_LOCATIONS` | | 取得対象地点の JSON 文字列（未設定時はデフォルト地点を使用） |

### `JMA_LOCATIONS` のフォーマット

```json
[
  {
    "area_name": "首都圏",
    "prec_no": 44,
    "prec_name": "東京",
    "block_no": 47662,
    "block_name": "東京"
  }
]
```

未設定の場合は `src/locations.toml` に定義されたデフォルト地点（東京・大阪・福岡・高松・広島・名古屋）が使用される。

## セットアップ

依存関係の管理には [uv](https://docs.astral.sh/uv/) を使用する。

```bash
# 依存関係のインストール
uv sync

# 開発用依存関係を含めてインストール
uv sync --group dev
```

## ローカル実行

### 過去5年分のデータを一括取得

`download_history.py` を実行すると、デフォルト地点の過去5年分のデータを取得して CSV に保存する。

```bash
uv run python src/download_history.py
```

出力ファイルは `data/weather_history_5y.csv` に保存される。

### テストの実行

```bash
uv run pytest
```

### Lint / フォーマット

```bash
# チェック
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# 自動修正
uv run ruff check --fix src/ tests/
uv run ruff format src/ tests/
```

## Cloud Run Jobs へのデプロイ

`main.py` を Cloud Run Jobs として実行することで、前日分のデータを取得して BigQuery へアップロードする。

### Docker イメージのビルドとプッシュ

```bash
export PROJECT_ID=<your-project-id>
export REGION=asia-northeast1
export IMAGE=gcr.io/${PROJECT_ID}/jma-scraper-observed

docker build -t ${IMAGE} .
docker push ${IMAGE}
```

### Cloud Run Jobs の作成

```bash
gcloud run jobs create jma-scraper-observed \
  --image ${IMAGE} \
  --region ${REGION} \
  --set-env-vars GCP_PROJECT_ID=${PROJECT_ID} \
  --set-env-vars BQ_DATASET_ID=weather_data \
  --set-env-vars BQ_TABLE_ID=daily_stats
```

### スケジュール実行（Cloud Scheduler）

```bash
gcloud scheduler jobs create http jma-scraper-observed-daily \
  --schedule "0 6 * * *" \
  --time-zone "Asia/Tokyo" \
  --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/jma-scraper-observed:run" \
  --oauth-service-account-email <your-service-account>@${PROJECT_ID}.iam.gserviceaccount.com
```

## プロジェクト構成

```
.
├── src/
│   ├── main.py              # Cloud Run Jobs エントリポイント
│   ├── download_history.py  # 過去5年分一括取得スクリプト
│   ├── scraper.py           # 気象庁スクレイパー
│   ├── bigquery_client.py   # BigQuery 操作
│   ├── config.py            # 環境変数・設定の取得
│   ├── models.py            # Pydantic モデル
│   └── locations.toml       # デフォルト観測地点設定
├── tests/                   # ユニットテスト
├── data/                    # CSV 出力先
├── Dockerfile
└── pyproject.toml
```
