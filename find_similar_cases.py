# -*- coding: utf-8 -*-
"""
全市場主力波段吸籌雷達 (Heavy Accumulation & Strong Broker Scanner)
===================================================================
功能：
1. 採用高效 DuckDB 引擎，秒級穿透全市場數千萬筆 Parquet 分點日資料
2. 自動識別類似「川湖 (2059) + 凱基-三多 (9275)」之主力大戶波段連續重押吸籌案例
3. 預設按「波段淨買超金額」由大到小排序，確保巨額重押之主力個股 (如川湖) 名列前茅
4. 日期格式統一為 YYYY-MM-DD (去除 00:00:00)
5. 產出全繁體中文欄位之視覺化終端報表與 Excel (.xlsx) / CSV 檔案
"""

import os
import sys
import glob
import json
import time
import argparse
from typing import List, Dict, Optional, Tuple

import requests
import duckdb
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def get_stock_name_map() -> Dict[str, str]:
    """取得股票代號對應中文名稱字典 (優先讀取快取)"""
    cache_path = os.path.join(os.path.dirname(__file__), "stock_name_map.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                name_map = json.load(f)
                if len(name_map) > 500:
                    return name_map
        except Exception:
            pass

    name_map = {}
    try:
        r = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=8)
        if r.status_code == 200:
            for d in r.json():
                c = str(d.get("Code", "")).strip()
                n = str(d.get("Name", "")).strip()
                if c and n:
                    name_map[c] = n
    except Exception:
        pass

    try:
        r = requests.get("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", timeout=8)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            for tr in soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    parts = tds[0].text.strip().split("\u3000")
                    if len(parts) >= 2:
                        c, n = parts[0].strip(), parts[1].strip()
                        if c and n and c not in name_map:
                            name_map[c] = n
    except Exception:
        pass

    try:
        r = requests.get("https://isin.twse.com.tw/isin/C_public.jsp?strMode=4", timeout=8)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            for tr in soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    parts = tds[0].text.strip().split("\u3000")
                    if len(parts) >= 2:
                        c, n = parts[0].strip(), parts[1].strip()
                        if c and n and c not in name_map:
                            name_map[c] = n
    except Exception:
        pass

    if len(name_map) > 100:
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(name_map, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    return name_map


def get_broker_name_map() -> Dict[str, str]:
    """取得券商分點代碼對應中文名稱字典 (優先讀取專案快取)"""
    cache_path = os.path.join(os.path.dirname(__file__), "broker_name_map.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                b_map = json.load(f)
                if len(b_map) > 100:
                    return b_map
        except Exception:
            pass

    candidates = [
        cache_path,
        os.path.join(os.path.dirname(__file__), "taiwan_stcok_securities_trader_info.csv"),
        os.path.join(os.path.dirname(__file__), "..", "StockBrokerPriceCorrelation", "data", "taiwan_stcok_securities_trader_info.csv"),
        "d:/MyProject/StockBrokerPriceCorrelation/data/taiwan_stcok_securities_trader_info.csv"
    ]
    broker_map = {}
    for c in candidates:
        if os.path.exists(c):
            try:
                df = pd.read_csv(c, encoding="utf-8")
                if "securities_trader_id" in df.columns and "securities_trader" in df.columns:
                    for _, row in df.iterrows():
                        b_id = str(row["securities_trader_id"]).strip()
                        b_name = str(row["securities_trader"]).strip()
                        if b_id and b_name:
                            broker_map[b_id] = b_name
                    break
            except Exception:
                pass

    if len(broker_map) > 50:
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(broker_map, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    return broker_map


def scan_heavy_accumulation(
    data_dir: str,
    min_net_amt_yi: float = 0.5,           # 最小波段淨買超金額 (億元，預設 0.5 億 = 5000 萬元)
    min_buy_ratio_pct: float = 75.0,       # 最小買進佔比 (買進股數 / (買進+賣出) >= 75%)
    min_net_vol_sheets: float = 100.0,     # 最小淨買超張數 (預設 100 張)
    min_trade_days: int = 10,              # 最少交易天數 (排除少數天數大宗短線)
    top_n: int = 30,
    sort_by: str = "amt",                  # 排序方式: "amt" (金額優先，預設) 或 "score" (純度評分優先)
    symbol_filter: Optional[str] = None,
    broker_filter: Optional[str] = None
) -> pd.DataFrame:
    """
    使用 DuckDB 掃描 Parquet 檔案並匯總主力吸籌案例 (輸出中文標的與中文欄位)
    """
    raw_files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    if not raw_files:
        print(f"[!] 於目錄 {data_dir} 未找到任何 Parquet 檔案。")
        return pd.DataFrame()

    files = [f.replace("\\", "/") for f in raw_files]
    print(f"==================================================")
    print(f"[*] 主力波段吸籌雷達啟動 (川湖-凱基三多模式掃描)")
    print(f"[*] 掃描檔案數: {len(files)} 個交易日 (全市場分點資料)")
    print(f"[*] 門檻條件: 淨買超 >= {min_net_amt_yi:.2f} 億元, 買進佔比 >= {min_buy_ratio_pct:.0f}%, 淨買超 >= {min_net_vol_sheets} 張, 交易天數 >= {min_trade_days} 天")
    print(f"[*] 排序基準: {'【淨買超總金額優先】' if sort_by == 'amt' else '【吸籌純度評分優先】'}")
    print(f"==================================================")
    sys.stdout.flush()

    stock_names = get_stock_name_map()
    broker_names = get_broker_name_map()

    start_t = time.time()

    extra_filter = ""
    if symbol_filter:
        extra_filter += f" AND symbol = '{symbol_filter.strip()}'"
    if broker_filter:
        extra_filter += f" AND broker_id = '{broker_filter.strip()}'"

    sql = f"""
    WITH filtered_trades AS (
        SELECT 
            symbol,
            broker_id,
            CAST(trade_date AS VARCHAR) AS trade_date,
            buy_vol,
            sell_vol,
            net_vol,
            buy_amt,
            sell_amt,
            net_amt,
            CASE WHEN net_vol > 0 THEN 1 ELSE 0 END AS is_buy_day
        FROM read_parquet({files})
        WHERE (buy_amt >= 200 OR sell_amt >= 200){extra_filter}
    ),
    agg AS (
        SELECT 
            symbol,
            broker_id,
            SUBSTRING(MIN(trade_date), 1, 10) AS first_date,
            SUBSTRING(MAX(trade_date), 1, 10) AS last_date,
            COUNT(DISTINCT trade_date) AS trade_days,
            SUM(is_buy_day) AS buy_days,
            SUM(buy_vol) / 1000.0 AS buy_vol_sheets,
            SUM(sell_vol) / 1000.0 AS sell_vol_sheets,
            SUM(net_vol) / 1000.0 AS net_vol_sheets,
            SUM(buy_amt) / 10000.0 AS buy_amt_yi,
            SUM(sell_amt) / 10000.0 AS sell_amt_yi,
            SUM(net_amt) / 10000.0 AS net_amt_yi,
            ROUND((SUM(buy_amt) * 1000.0) / NULLIF(SUM(buy_vol), 0), 2) AS buy_avg_price,
            ROUND((SUM(sell_amt) * 1000.0) / NULLIF(SUM(sell_vol), 0), 2) AS sell_avg_price,
            ROUND(SUM(buy_vol) * 100.0 / NULLIF(SUM(buy_vol) + SUM(sell_vol), 0), 1) AS buy_ratio_pct,
            ROUND(SUM(is_buy_day) * 100.0 / NULLIF(COUNT(DISTINCT trade_date), 0), 1) AS buy_day_pct
        FROM filtered_trades
        GROUP BY symbol, broker_id
    )
    SELECT 
        symbol,
        broker_id,
        first_date,
        last_date,
        trade_days,
        buy_days,
        buy_day_pct,
        ROUND(buy_vol_sheets, 1) AS buy_vol_sheets,
        ROUND(sell_vol_sheets, 1) AS sell_vol_sheets,
        ROUND(net_vol_sheets, 1) AS net_vol_sheets,
        buy_ratio_pct,
        buy_avg_price,
        sell_avg_price,
        ROUND(buy_amt_yi, 2) AS buy_amt_yi,
        ROUND(net_amt_yi, 2) AS net_amt_yi
    FROM agg
    WHERE net_amt_yi >= {min_net_amt_yi}
      AND buy_ratio_pct >= {min_buy_ratio_pct}
      AND net_vol_sheets >= {min_net_vol_sheets}
      AND trade_days >= {min_trade_days}
    """

    res_df = duckdb.query(sql).to_df()
    elapsed = time.time() - start_t
    print(f"[OK] 掃描完成！耗時: {elapsed:.2f} 秒，共篩選出 {len(res_df):,} 組「個股＋主力分點」重押組合")
    print(f"==================================================")

    if res_df.empty:
        return res_df

    res_df["first_date"] = res_df["first_date"].astype(str).str.slice(0, 10)
    res_df["last_date"] = res_df["last_date"].astype(str).str.slice(0, 10)

    # 計算吸籌強度評分 Score (0 ~ 100 分)
    amt_score = np.clip(np.log10(np.maximum(1.0, res_df["net_amt_yi"] * 10000.0)) * 8.0, 0, 40.0)
    ratio_score = np.clip((res_df["buy_ratio_pct"] / 100.0 - 0.5) * 60.0, 0, 30.0)
    day_score = np.clip(res_df["buy_day_pct"] * 0.3, 0, 30.0)
    res_df["score"] = (amt_score + ratio_score + day_score).round(1)

    res_df["股票標的"] = res_df["symbol"].apply(lambda s: f"{s}-{stock_names.get(s, '未知')}")
    res_df["券商分點"] = res_df["broker_id"].apply(lambda b: f"{b}-{broker_names.get(b, '未知分點')}")

    chinese_col_map = {
        "first_date": "起算日期",
        "last_date": "最新活躍日",
        "trade_days": "進出天數",
        "buy_days": "買超天數",
        "buy_day_pct": "買超天數佔比(%)",
        "buy_vol_sheets": "累計買進(張)",
        "sell_vol_sheets": "累計賣出(張)",
        "net_vol_sheets": "累計淨買超(張)",
        "buy_ratio_pct": "買進純度佔比(%)",
        "buy_avg_price": "買進均價/主力成本(元)",
        "sell_avg_price": "賣出均價(元)",
        "buy_amt_yi": "買進總金額(億元)",
        "net_amt_yi": "淨買超金額(億元)",
        "score": "主力吸籌強度評分"
    }
    res_df.rename(columns=chinese_col_map, inplace=True)

    ordered_cols = [
        "股票標的", "券商分點", "起算日期", "最新活躍日", "進出天數",
        "買超天數", "買超天數佔比(%)", "累計買進(張)", "累計賣出(張)",
        "累計淨買超(張)", "買進純度佔比(%)", "買進均價/主力成本(元)",
        "賣出均價(元)", "買進總金額(億元)", "淨買超金額(億元)", "主力吸籌強度評分"
    ]
    
    # 根據排序參數決定主排序欄位
    if sort_by == "score":
        res_df = res_df[ordered_cols].sort_values(by=["主力吸籌強度評分", "淨買超金額(億元)"], ascending=[False, False]).reset_index(drop=True)
    else:
        # 預設按「淨買超金額」由大到小排序，確保巨資重押之個股 (如川湖) 優先浮現
        res_df = res_df[ordered_cols].sort_values(by=["淨買超金額(億元)", "主力吸籌強度評分"], ascending=[False, False]).reset_index(drop=True)

    return res_df.head(top_n)


def save_report_safely(df: pd.DataFrame, target_path: str):
    """安全儲存報表 (確保日期以文字格式寫入，若檔案被 Excel 鎖定則自動寫入備用檔名)"""
    os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)
    df_save = df.copy()
    if "起算日期" in df_save.columns:
        df_save["起算日期"] = df_save["起算日期"].astype(str)
    if "最新活躍日" in df_save.columns:
        df_save["最新活躍日"] = df_save["最新活躍日"].astype(str)

    try:
        if target_path.endswith(".csv"):
            df_save.to_csv(target_path, index=False, encoding="utf-8-sig")
        else:
            df_save.to_excel(target_path, index=False, engine="openpyxl")
        print(f"\n[+] 分析報告已成功輸出至: {target_path}")
    except PermissionError:
        base, ext = os.path.splitext(target_path)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        alt_path = f"{base}_{timestamp}{ext}"
        if alt_path.endswith(".csv"):
            df_save.to_csv(alt_path, index=False, encoding="utf-8-sig")
        else:
            df_save.to_excel(alt_path, index=False, engine="openpyxl")
        print(f"\n[!] 原始檔案 {target_path} 正被其他軟體開啟鎖定，已另存新檔至: {alt_path}")


def main():
    parser = argparse.ArgumentParser(description="全市場主力波段吸籌雷達 (川湖-凱基三多模式掃描器)")
    parser.add_argument("--data-dir", type=str, default=None, help="Parquet 資料夾路徑")
    parser.add_argument("--min-amt", type=float, default=0.5, help="最小波段淨買超金額 (億元，預設 0.5 億 = 5000 萬)")
    parser.add_argument("--min-ratio", type=float, default=75.0, help="最小買進佔比 (百分比，預設 75 即 75%)")
    parser.add_argument("--min-sheets", type=float, default=100.0, help="最小淨買超張數 (預設 100 張)")
    parser.add_argument("--min-days", type=int, default=10, help="最少進出交易天數 (預設 10 天，排除少數天數大宗短線)")
    parser.add_argument("--sort", type=str, default="amt", choices=["amt", "score"], help="排序方式: amt (淨買金額優先，預設) 或 score (純度評分優先)")
    parser.add_argument("--symbol", type=str, default=None, help="指定查詢特定股票 (例: 2059)")
    parser.add_argument("--broker", type=str, default=None, help="指定查詢特定券商分點 (例: 9275)")
    parser.add_argument("--top", type=int, default=30, help="輸出前幾大名單 (預設 30)")
    parser.add_argument("--output", type=str, default=None, help="輸出報告路徑 (.xlsx 或 .csv)")

    args = parser.parse_args()

    data_dir = args.data_dir
    if not data_dir:
        candidates = [
            os.path.join(os.path.dirname(__file__), "20260822分點資料"),
            os.path.join(os.path.dirname(__file__), "output"),
            os.path.join(os.path.dirname(__file__), "..", "dataProject", "20260822分點資料")
        ]
        for c in candidates:
            if os.path.exists(c) and glob.glob(os.path.join(c, "*.parquet")):
                data_dir = c
                break

    if not data_dir:
        print("[!] 找不到 Parquet 資料目錄，請以 --data-dir 指定。")
        return

    df_top = scan_heavy_accumulation(
        data_dir=data_dir,
        min_net_amt_yi=args.min_amt,
        min_buy_ratio_pct=args.min_ratio,
        min_net_vol_sheets=args.min_sheets,
        min_trade_days=args.min_days,
        top_n=args.top,
        sort_by=args.sort,
        symbol_filter=args.symbol,
        broker_filter=args.broker
    )

    if not df_top.empty:
        print("\n" + "=" * 115)
        print(f"🏆 全市場主力波段重押排行榜 TOP {len(df_top)}（類川湖-凱基三多型態）：")
        print("=" * 115)
        
        display_cols = [
            "股票標的", "券商分點", "起算日期", "最新活躍日", "進出天數",
            "累計買進(張)", "累計淨買超(張)", "買進純度佔比(%)", "買進均價/主力成本(元)", "淨買超金額(億元)", "主力吸籌強度評分"
        ]
        print(df_top[display_cols].to_string(index=True))

        out_path = args.output
        if not out_path:
            timestamp_str = time.strftime("%Y%m%d_%H%M%S")
            out_path = os.path.join(os.path.dirname(__file__), "output", f"heavy_accumulation_cases_{timestamp_str}.xlsx")

        save_report_safely(df_top, out_path)


if __name__ == "__main__":
    main()
