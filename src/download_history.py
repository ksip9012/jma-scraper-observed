"""過去5年分の気象データを一括で取得し、ローカルのCSVに保存するスクリプト。

src/main.py のロジックを使用してバリデーション済みのデータを収集します。
"""

import logging
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from main import fetch_and_validate_weather

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_5years_history():
    """現在から遡って5年分のデータを取得し、1つのCSVに保存する。"""
    
    # 地点設定（デフォルト: 東京）
    prec_no = 44
    prec_name = "東京"
    block_no = 47662
    block_name = "東京"
    
    # 5年分の月数を計算（60ヶ月）
    # 実行月の前月（または今月）から遡る
    end_date = datetime.now()
    start_date = end_date - relativedelta(years=5)
    
    logger.info("%s から %s までのデータを取得します。", 
                start_date.strftime("%Y/%m"), 
                end_date.strftime("%Y/%m"))
    
    all_dfs = []
    current_date = start_date
    
    while current_date <= end_date:
        year, month = current_date.year, current_date.month
        
        try:
            # main.py のバリデーション済み取得関数を使用
            df = fetch_and_validate_weather(
                prec_no, prec_name, block_no, block_name, year, month
            )
            
            if not df.empty:
                all_dfs.append(df)
                logger.info("%04d/%02d: %d件のデータを取得完了", 
                            year, month, len(df))
            else:
                logger.warning("%04d/%02d: データがありませんでした", year, month)
                
            # サーバー負荷軽減のための待機
            time.sleep(1)
            
        except Exception as e:
            logger.error("%04d/%02d の取得中にエラーが発生しました: %s", 
                         year, month, e)
        
        # 次の月へ
        current_date += relativedelta(months=1)

    if all_dfs:
        # すべてのデータを結合
        master_df = pd.concat(all_dfs, ignore_index=True)
        
        # CSVファイルに出力
        output_file = "weather_history_5y.csv"
        # utf-8-sig を使うとExcelで開いた際の文字化けを防げます
        master_df.to_csv(output_file, index=False, encoding="utf-8-sig")
        
        logger.info("-" * 30)
        logger.info("全ての処理が完了しました。")
        logger.info("保存先: %s", output_file)
        logger.info("合計レコード数: %d 行", len(master_df))
    else:
        logger.error("データが1件も取得できませんでした。")

if __name__ == "__main__":
    download_5years_history()

if __name__ == "__main__":
    download_5years_history()
