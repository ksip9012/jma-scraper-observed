# Claude へのプロジェクト指示

## Git/GitHub ワークフロー

### 基本ルール
- `main` への直接コミット・プッシュは行わない
- issue に対応する際は必ずブランチを切ってから作業を開始する
- 作業完了後は必ず PR を作成し、ユーザーにマージを委ねる
- マージ・ブランチ削除はユーザーからの完了報告を受けてから行う

### ブランチ命名規則
```
<type>/issue-<番号>-<概要>
```

| type | 用途 |
|---|---|
| `feat` | 新機能の追加 |
| `fix` | バグ修正 |
| `docs` | ドキュメントのみの変更 |
| `refactor` | 機能変更を伴わないコード整理 |
| `chore` | ビルド・設定ファイルの変更 |

例: `fix/issue-4-remove-duplicate-main`, `feat/issue-5-multi-location`

### コミットメッセージ規則
[Conventional Commits](https://www.conventionalcommits.org/) に従う。

```
<type>: <概要> (#<issue番号>)
```

例:
```
fix: download_history.py の重複した __main__ ブロックを削除 (#4)
feat: weather_ingestion_handler を複数地点対応にする (#5)
```

### PR 作成ルール
- ベースブランチは `main`
- 本文に `Close #<issue番号>` を記載して issue と紐づける
- マージはユーザーが GitHub 上で行う
