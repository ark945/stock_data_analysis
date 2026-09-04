# -*- coding: utf-8 -*-
"""
夜間極速衍生指標入庫執行器 (Nightly Derivatives Sync Runner)
========================================================================
專為 22:00 證交所/櫃買中心官方資券公布後設計之輕量極速執行模組：
1. 僅下載當日所需 5 個輕量檔案 (分點、收盤價、融資券、期貨、集保)，耗時 < 10 秒。
2. 執行 chip_derivatives_engine 運算「🚀 極品軋空、🩸 散戶接刀套牢坑、💎 籌碼集中度、大盤期權避震」。
3. 採用表層級安全清空防護，將全新衍生戰情報告精準寫入 Supabase chip_derivatives_signals 表。
4. 同步更新當日 chip_accumulation_signals 的 short_margin_ratio_pct (券資比%)。
5. 全流程僅需 15~25 秒，極速不耗 GitHub Actions 額度。
"""

import os
import sys
import glob
import json
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
import pandas as pd
import numpy as np

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

TAIPEI_TZ = timezone(timedelta(hours=8))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from chip_derivatives_engine import run_derivatives_analysis_for_date, get_stock_name_map
from sync_to_mystock import clean_nan_and_inf, upsert_to_supabase, purge_date_from_supabase


def download_minimal_derivatives_files(target_date: str) -> bool:
    """僅自 Google Drive 下載目標交易日所需之輕量檔案"""
    is_gh = os.environ.get("GITHUB_ACTIONS") == "true"
    folder_id = os.environ.get("GDRIVE_FOLDER_ID")
    if not is_gh and not folder_id:
        print("[*] 本機環境或未配置 GDRIVE_FOLDER_ID，略過雲端檔案下載。")
        return True

    print(f"[*] 正在從 Google Drive 極速拉取 {target_date} 衍生分析所需檔案...")
    try:
        from cloud_gdrive_downloader import (
            download_recent_parquet_files,
            download_recent_close_price_files,
            download_recent_files_by_pattern
        )
        # 1. 當日分點
        download_recent_parquet_files(lookback_days=1, dest_dir="./temp_cache_parquet")
        # 2. 當日收盤價
        download_recent_close_price_files(lookback_days=1, dest_dir="./temp_cache_close")
        # 3. 當日融資融券
        download_recent_files_by_pattern("margin", lookback_days=1, dest_dir="./output_margin")
        # 4. 當日期交所期權
        download_recent_files_by_pattern("taifex", lookback_days=1, dest_dir="./output_taifex")
        # 5. 最新集保股權
        download_recent_files_by_pattern("tdcc", lookback_days=1, dest_dir="./output_tdcc")
        print(f"[✓] 雲端輕量檔案下載完畢！")
        return True
    except Exception as e:
        print(f"[!] 雲端檔案下載過程提示: {e}")
        return False


def run_nightly_derivatives_sync(target_date: Optional[str] = None, data_dir: Optional[str] = None):
    """執行夜間衍生指標極速運算與 Supabase 戰情室同步"""
    today_str = datetime.now(timezone.utc).astimezone(TAIPEI_TZ).strftime("%Y-%m-%d")
    actual_date = target_date or today_str

    print("=" * 65)
    print(f"⚡ 夜間籌碼衍生指標極速同步引擎啟動 (基準日: {actual_date})")
    print("=" * 65)

    # 1. 下載極小集合檔案
    if not data_dir or not os.path.exists(data_dir):
        download_minimal_derivatives_files(actual_date)
        broker_dir = "./temp_cache_parquet" if os.path.exists("./temp_cache_parquet") else "./output"
    else:
        broker_dir = data_dir

    # 2. 執行衍生指標核心量化運算
    print(f"[*] 正在運算 {actual_date} 籌碼衍生指標 (軋空/接刀/集中度/大盤避震)...")
    out_reports_dir = "./daily_reports"
    os.makedirs(out_reports_dir, exist_ok=True)
    d_res = run_derivatives_analysis_for_date(
        trade_date=actual_date,
        broker_dir=broker_dir,
        output_dir=out_reports_dir
    )

    if not d_res:
        print(f"[!] {actual_date} 衍生指標運算查無結果。")
        return False

    # 3. 組合 Supabase chip_derivatives_signals 格式
    stock_name_cache = get_stock_name_map()
    from find_similar_cases import get_stock_market_map
    stock_market_cache = get_stock_market_map()
    deriv_rows = []

    def _parse_deriv_row(r, sig_type, default_tag, default_guide):
        sym = str(r.get("symbol", ""))
        name = str(r.get("stock_name") or r.get("name") or "")
        if not name or name == "未知":
            name = stock_name_cache.get(sym, sym)

        raw_market = r.get("market")
        if pd.isna(raw_market) or not raw_market or str(raw_market).lower() in ["nan", "none", "null", ""]:
            market = stock_market_cache.get(sym, "上櫃" if (len(sym) == 4 and sym.startswith(("4", "5", "6", "7", "8"))) else "上市")
        else:
            raw_str = str(raw_market)
            if "TWSE" in raw_str.upper() or "上市" in raw_str:
                market = "上市"
            elif "TPEX" in raw_str.upper() or "上櫃" in raw_str:
                market = "上櫃"
            elif "興櫃" in raw_str:
                market = "興櫃"
            else:
                market = stock_market_cache.get(sym, raw_str)

        close_val = float(r["close"]) if pd.notna(r.get("close")) else (float(r["close_price"]) if pd.notna(r.get("close_price")) else None)
        short_ratio = float(r["short_margin_ratio_pct"]) if pd.notna(r.get("short_margin_ratio_pct")) else None
        m_net = float(r["margin_net"]) if pd.notna(r.get("margin_net")) else None
        s_net = float(r["short_net"]) if pd.notna(r.get("short_net")) else None

        diff_cnt = None
        if pd.notna(r.get("diff_broker_count")):
            diff_cnt = float(r["diff_broker_count"])
        elif pd.notna(r.get("broker_diff")):
            diff_cnt = float(r["broker_diff"])

        large_pct = float(r["large_shareholder_pct"]) if pd.notna(r.get("large_shareholder_pct")) else None
        retail_pct = float(r["retail_shareholder_pct"]) if pd.notna(r.get("retail_shareholder_pct")) else None

        return {
            "trade_date": actual_date,
            "signal_type": sig_type,
            "symbol": sym,
            "stock_name": name,
            "market": market,
            "close_price": close_val,
            "short_margin_ratio_pct": short_ratio,
            "margin_net": m_net,
            "short_net": s_net,
            "diff_broker_count": diff_cnt,
            "large_shareholder_pct": large_pct,
            "retail_shareholder_pct": retail_pct,
            "persona_tag": default_tag,
            "action_guide": default_guide
        }

    if not d_res.get("squeeze", pd.DataFrame()).empty:
        for _, sr in d_res["squeeze"].head(30).iterrows():
            deriv_rows.append(_parse_deriv_row(sr, "squeeze", "🚀 極品軋空", "高券資比+融券暴增+家數差集中，空頭回補爆發力強"))
    if not d_res.get("trap", pd.DataFrame()).empty:
        for _, tr in d_res["trap"].head(30).iterrows():
            deriv_rows.append(_parse_deriv_row(tr, "trap", "⚠️ 散戶接刀", "融資暴增+主力倒貨+家數差擴大，散戶接刀套牢風險極高"))
    if not d_res.get("concentrated", pd.DataFrame()).empty:
        for _, cr in d_res["concentrated"].head(30).iterrows():
            deriv_rows.append(_parse_deriv_row(cr, "concentrated", "💎 籌碼極度集中", "買賣家數差大幅負值，少數主力分點積極收納籌碼"))

    clean_rows = clean_nan_and_inf(deriv_rows)
    print(f"[✓] 成功產製 {len(clean_rows)} 筆最新籌碼衍生戰情報告！")

    # 4. 寫入 Supabase 資料庫
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if supabase_url and supabase_key:
        print("[*] 正在將衍生指標同步至 myStock 雲端戰情室...")
        if clean_rows:
            # 安全依表清空：只清空當天 chip_derivatives_signals，不碰其他任何資料表與歷史日期
            purge_date_from_supabase(supabase_url, supabase_key, actual_date, tables=["chip_derivatives_signals"])
            upsert_to_supabase(
                supabase_url,
                supabase_key,
                "chip_derivatives_signals",
                clean_rows,
                on_conflict="trade_date,signal_type,symbol"
            )
            print(f"[✓] 成功同步 {len(clean_rows)} 筆衍生指標至 Supabase (chip_derivatives_signals)！")

            # 5. 若融資券就緒，順帶更新 chip_accumulation_signals 的券資比 short_margin_ratio_pct
            from chip_derivatives_engine import load_margin_data
            margin_dirs = [
                "./output_margin",
                broker_dir,
                r"d:\MyProject\stock_data_downloader\output_margin",
                "../stock_data_downloader/output_margin",
            ]
            mdf = load_margin_data(actual_date, margin_dirs)
            if not mdf.empty:
                try:
                    margin_map = {str(r["symbol"]): float(r.get("short_margin_ratio_pct", 0) or 0) for _, r in mdf.iterrows()}
                    # 查詢當日已存在的吸籌訊號 (查詢所有欄位，全欄位更新避免 not-null constraint)
                    import urllib.request
                    query_url = f"{supabase_url.rstrip('/')}/rest/v1/chip_accumulation_signals?trade_date=eq.{actual_date}&select=*"
                    q_headers = {"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}"}
                    q_req = urllib.request.Request(query_url, headers=q_headers)
                    with urllib.request.urlopen(q_req, timeout=10) as resp:
                        accum_data = json.loads(resp.read().decode())
                    if accum_data:
                        update_payload = []
                        for row in accum_data:
                            sym = str(row.get("symbol", ""))
                            if sym in margin_map:
                                row["short_margin_ratio_pct"] = margin_map[sym]
                                update_payload.append(row)
                        if update_payload:
                            clean_payload = clean_nan_and_inf(update_payload)
                            upsert_to_supabase(
                                supabase_url,
                                supabase_key,
                                "chip_accumulation_signals",
                                clean_payload,
                                on_conflict="trade_date,period_days,symbol,broker_name"
                            )
                            print(f"[✓] 成功對齊並更新 {len(clean_payload)} 筆吸籌卡片最新券資比！")
                except Exception as _m_err:
                    print(f"[!] 吸籌卡片券資比對齊提示: {_m_err}")
        else:
            print("[!] 防呆攔截：本次無衍生指標產出，保留線上既有資料，拒絕清空！")
    else:
        print("[!] 未配置 SUPABASE_URL 或 SUPABASE_KEY，略過雲端資料庫寫入。")

    print(f"🎉 【{actual_date}】夜間衍生指標極速入庫流程順利完成！")
    return True


def main():
    parser = argparse.ArgumentParser(description="夜間極速衍生指標入庫執行器")
    parser.add_argument("--date", default="", help="指定交易日 (YYYY-MM-DD，留空則自動處理最新當天)")
    parser.add_argument("--data-dir", default="", help="本機資料目錄 (選用)")
    args = parser.parse_args()

    run_nightly_derivatives_sync(target_date=args.date if args.date else None, data_dir=args.data_dir if args.data_dir else None)


if __name__ == "__main__":
    main()
