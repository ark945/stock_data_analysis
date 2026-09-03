# -*- coding: utf-8 -*-
"""
myStock 雲端籌碼戰情室資料同步模組 (Sync to myStock Supabase)
===========================================================
核心功能：
1. 聚合全市場分點與收盤價量化運算成果：
   - 主力四週期吸籌總表 (5日短線點火 / 10日雙週波段 / 20日月波段川湖 / 60日季線長莊)
   - 主力出貨逃離與散戶接盤下車表
   - 外資各大席位與本土法人部重押表
   - 尾盤放量站上 VWAP 逆向歸因表
   - 每日全市場多空司令與宏觀多空結論
2. 透過 Supabase REST API (HTTPS) 執行高效秒級 Upsert，驅動 myStock 雲端戰情室。
3. 支援 `--dry-run` 模式，便於本機檢驗資料格式與結構。
"""

import os
import sys
import glob
import json
import argparse
from typing import Dict, List, Any, Optional
import urllib.request
import urllib.error

import pandas as pd
import numpy as np

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 引入本專案運算模組
from cloud_report_generator import run_heavy_accumulation_analysis
from find_exit_cases import scan_exit_distribution
from institutional_broker_rankings import run_institutional_ranking_analysis
from reverse_broker_matcher import scan_tail_vwap_and_attribute
from send_email_report import send_telegram_notify

import re

def extract_date_from_filename(fname: str) -> str:
    m = re.search(r"\d{4}-\d{2}-\d{2}", fname)
    return m.group(0) if m else "9999-99-99"


def find_local_files(data_dir: str):
    search_dirs = [data_dir, "./temp_cache_parquet", "./cloud_data", "./20260822分點資料"]
    parquet_files = []
    close_files_all = []
    for d in search_dirs:
        if os.path.exists(d):
            parquet_files.extend(glob.glob(os.path.join(d, "*.parquet")))
            close_files_all.extend(glob.glob(os.path.join(d, "api_close1_*.parquet")))
    absr1_files = [
        f for f in parquet_files 
        if "finmind" not in os.path.basename(f).lower() and "close1" not in os.path.basename(f).lower()
    ]
    absr1_files = sorted(list(set(absr1_files)), key=lambda x: extract_date_from_filename(os.path.basename(x)))
    close_files_all = sorted(list(set(close_files_all)), key=lambda x: extract_date_from_filename(os.path.basename(x)))
    return absr1_files, close_files_all

# 嘗試載入 .env
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass


def clean_nan_and_inf(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """清理 JSON 序列化不支援的 NaN 與 Inf"""
    cleaned = []
    for r in records:
        item = {}
        for k, v in r.items():
            if pd.isna(v) or v is None:
                item[k] = None
            elif isinstance(v, (float, np.floating)):
                if np.isnan(v) or np.isinf(v):
                    item[k] = None
                else:
                    item[k] = round(float(v), 2)
            elif isinstance(v, (int, np.integer)):
                item[k] = int(v)
            else:
                item[k] = str(v)
        cleaned.append(item)
    return cleaned


def deduplicate_records(records: List[Dict[str, Any]], key_cols: List[str]) -> List[Dict[str, Any]]:
    """依據指定欄位組合去除重複資料，避免 Postgres 21000 錯誤"""
    seen = set()
    deduped = []
    for r in records:
        key = tuple(str(r.get(col, "")) for col in key_cols)
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    return deduped


def upsert_to_supabase(
    supabase_url: str,
    supabase_key: str,
    table_name: str,
    records: List[Dict[str, Any]],
    on_conflict: str = ""
) -> bool:
    """透過 Supabase PostgREST API 進行批次 Upsert"""
    if not records:
        print(f"[*] 表 {table_name}: 無資料需同步。")
        return True

    # 批次內去重，防止 Postgres 報錯 "ON CONFLICT DO UPDATE command cannot affect row a second time"
    if on_conflict:
        key_cols = [c.strip() for c in on_conflict.split(",") if c.strip()]
        records = deduplicate_records(records, key_cols)

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{table_name}"
    if on_conflict:
        endpoint += f"?on_conflict={on_conflict}"

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    data_bytes = json.dumps(records, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(endpoint, data=data_bytes, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status in [200, 201]:
                print(f"[✓] 表 {table_name}: 成功同步 {len(records)} 筆資料至 Supabase。")
                return True
            else:
                print(f"[!] 表 {table_name}: 回應代碼 {resp.status}")
                return False
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8", errors="replace")
        print(f"[✗] 表 {table_name} 同步失敗 HTTP {e.code}: {err_msg}")
        return False
    except Exception as e:
        print(f"[✗] 表 {table_name} 同步連線異常: {e}")
        return False


def prepare_chip_payloads(
    data_dir: str,
    target_date: Optional[str] = None
) -> Dict[str, Any]:
    """執行全套量化運算並組裝符合 Supabase Schema 的 Payload"""
    absr1_files, close_files_all = find_local_files(data_dir)
    if not absr1_files:
        raise FileNotFoundError(f"於目錄 {data_dir} 未找到分點 Parquet 檔案。")

    latest_file = absr1_files[-1]
    actual_date = target_date or extract_date_from_filename(os.path.basename(latest_file))

    # 若指定日期，截斷檔案至指定交易日為止
    absr1_files = [f for f in absr1_files if extract_date_from_filename(os.path.basename(f)) <= actual_date]
    close_files_all = [f for f in close_files_all if extract_date_from_filename(os.path.basename(f)) <= actual_date]

    print("=" * 65)
    print(f"[*] 正在從量化核心提取全市場籌碼情報 (基準交易日: {actual_date})")
    print(f"[*] 可用歷史窗口: {len(absr1_files)} 個交易日 (起: {extract_date_from_filename(os.path.basename(absr1_files[0]))} ~ 訖: {actual_date})")
    print("=" * 65)

    # 切分 4 個週期檔案清單
    total_files = len(absr1_files)
    files_5d = absr1_files[-5:] if total_files >= 5 else absr1_files
    files_10d = absr1_files[-10:] if total_files >= 10 else absr1_files
    files_20d = absr1_files[-20:] if total_files >= 20 else absr1_files
    files_60d = absr1_files[-60:] if total_files >= 60 else absr1_files

    total_close = len(close_files_all)
    close_5d = close_files_all[-5:] if total_close >= 5 else close_files_all
    close_10d = close_files_all[-10:] if total_close >= 10 else close_files_all
    close_20d = close_files_all[-20:] if total_close >= 20 else close_files_all
    close_60d = close_files_all[-60:] if total_close >= 60 else close_files_all

    # 1. 四週期吸籌總表
    print("[1/4] 運算主力四週期吸籌總表 (5d/10d/20d/60d)...")
    accum_configs = [
        (5, files_5d, close_5d, 0.2, 70.0, 30.0, 1),
        (10, files_10d, close_10d, 0.35, 72.0, 50.0, 2),
        (20, files_20d, close_20d, 0.5, 75.0, 80.0, 3),
        (60, files_60d, close_60d, 1.0, 75.0, 150.0, 5)
    ]
    accum_rows = []
    for p, p_files, c_files, min_amt, min_ratio, min_vol, min_days in accum_configs:
        df_p, _ = run_heavy_accumulation_analysis(
            parquet_files=p_files,
            min_net_amt_yi=min_amt,
            min_buy_ratio_pct=min_ratio,
            min_net_vol_sheets=min_vol,
            min_trade_days=min_days,
            close_price_files=c_files,
            top_n=30
        )
        if not df_p.empty:
            for _, r in df_p.head(50).iterrows():
                accum_rows.append({
                    "trade_date": actual_date,
                    "period_days": int(p),
                    "symbol": str(r.get("symbol", "")),
                    "stock_name": str(r.get("stock_name", "")),
                    "market": str(r.get("market", "上市")),
                    "broker_id": str(r.get("broker_id", "")),
                    "broker_name": str(r.get("broker_name", "")),
                    "net_amt_yi": float(r.get("net_amt_yi", 0)),
                    "net_vol_sheets": float(r.get("net_vol_sheets", 0)),
                    "buy_avg_price": float(r.get("buy_avg_price", 0)) if pd.notna(r.get("buy_avg_price")) else None,
                    "close_price": float(r.get("latest_close", 0)) if pd.notna(r.get("latest_close")) else None,
                    "cost_deviation_pct": float(r.get("cost_deviation_pct", 0)) if pd.notna(r.get("cost_deviation_pct")) else None,
                    "buy_purity_pct": float(r.get("buy_ratio_pct", 0)) if pd.notna(r.get("buy_ratio_pct")) else None,
                    "concentration_pct": float(r.get("concentration_pct", 0)) if pd.notna(r.get("concentration_pct")) else None,
                    "backtest_win_rate": float(r.get("backtest_win_rate", 0)) if pd.notna(r.get("backtest_win_rate")) else None,
                    "backtest_avg_return_pct": float(r.get("backtest_avg_return_pct", 0)) if pd.notna(r.get("backtest_avg_return_pct")) else None,
                    "persona_tag": "💎 波段主力" if p >= 20 else "⚡ 短線主力",
                    "action_guide": "主力重押鎖碼，順勢跟隨" if p >= 20 else "短線點火爆量，注意開高震盪"
                })

    # 2. 主力出貨逃離下車表
    print("[2/4] 運算主力出貨逃離與散戶接盤下車表...")
    from find_similar_cases import get_stock_name_map
    s_names = get_stock_name_map()
    df_exit = scan_exit_distribution(
        data_dir=data_dir,
        long_days=20,
        recent_days=5,
        min_long_net_amt_yi=0.3,
        min_recent_sell_amt_yi=0.2,
        top_n=30,
        target_date=actual_date
    )
    exit_rows = []
    if not df_exit.empty:
        for _, r in df_exit.iterrows():
            sym = str(r.get("symbol", ""))
            sname = s_names.get(sym, sym)
            exit_rows.append({
                "trade_date": actual_date,
                "exit_type": "20d基期-5d出貨",
                "symbol": sym,
                "stock_name": sname,
                "market": str(r.get("市場別", "上市")),
                "dump_broker_name": str(r.get("主力分點", "")),
                "dump_amt_yi": float(abs(r.get("近期淨賣超金額(億元)", 0))),
                "dump_vol_sheets": float(abs(r.get("近期淨賣超(張)", 0))),
                "sell_avg_price": float(r.get("近期賣出均價(元)", 0)) if pd.notna(r.get("近期賣出均價(元)")) else None,
                "close_price": float(r.get("近期賣出均價(元)", 0)) if pd.notna(r.get("近期賣出均價(元)")) else None,
                "retail_broker_name": "散戶多點接盤",
                "warning_level": f"{r.get('出貨型態', '出貨')} (危險分: {r.get('出貨危險評分', 0)})",
                "action_guide": "大戶翻臉連續賣超，出貨嚴重度高，切勿盲目接刀"
            })

    # 3. 外資席位與本土法人重押
    print("[3/4] 運算外資席位與本土法人部重押穿透...")
    df_foreign, df_inst, df_market_kings, _ = run_institutional_ranking_analysis(
        data_dir=data_dir,
        target_date=actual_date
    )
    inst_rows = []
    if not df_foreign.empty:
        for _, r in df_foreign.iterrows():
            inst_rows.append({
                "trade_date": actual_date,
                "category": "FOREIGN",
                "broker_name": str(r.get("券商分點", "")),
                "symbol": str(r.get("股票代號", "")),
                "stock_name": str(r.get("股票名稱", "")),
                "market": str(r.get("市場別", "上市")),
                "net_amt_yi": float(r.get("net_amt_yi", 0)),
                "net_sheets": float(r.get("net_sheets", 0)),
                "buy_avg_price": float(r.get("buy_avg_price", 0)) if pd.notna(r.get("buy_avg_price")) else None,
                "buy_purity_pct": float(r.get("buy_purity_pct", 0)) if pd.notna(r.get("buy_purity_pct")) else None,
                "feature_tag": str(r.get("外資屬性", "外資席位"))
            })
    if not df_inst.empty:
        for _, r in df_inst.iterrows():
            inst_rows.append({
                "trade_date": actual_date,
                "category": "DOMESTIC_INST",
                "broker_name": str(r.get("券商分點", "")),
                "symbol": str(r.get("股票代號", "")),
                "stock_name": str(r.get("股票名稱", "")),
                "market": str(r.get("市場別", "上市")),
                "net_amt_yi": float(r.get("net_amt_yi", 0)),
                "net_sheets": float(r.get("net_sheets", 0)),
                "buy_avg_price": float(r.get("buy_avg_price", 0)) if pd.notna(r.get("buy_avg_price")) else None,
                "buy_purity_pct": float(r.get("buy_purity_pct", 0)) if pd.notna(r.get("buy_purity_pct")) else None,
                "feature_tag": str(r.get("法人標籤", "本土法人"))
            })

    # 4. 執行尾盤放量 VWAP 歸因
    print("[4/4] 運算尾盤放量站上 VWAP 與主力分點逆向歸因...")
    df_vwap = scan_tail_vwap_and_attribute(
        data_dir=data_dir,
        target_date=actual_date
    )
    vwap_rows = []
    if not df_vwap.empty:
        for _, r in df_vwap.iterrows():
            vwap_rows.append({
                "trade_date": actual_date,
                "symbol": str(r.get("symbol", "")),
                "stock_name": str(r.get("股票名稱", "")),
                "market": str(r.get("市場別", "上市")),
                "close_price": float(r.get("close", 0)),
                "vwap_price": float(r.get("vwap", 0)),
                "vwap_premium_pct": float(r.get("vwap_premium_pct", 0)),
                "broker_name": str(r.get("券商分點", "")),
                "broker_buy_avg": float(r.get("buy_avg_price", 0)),
                "net_amt_yi": float(r.get("broker_net_amt_yi", 0)),
                "net_vol_sheets": float(r.get("broker_net_vol_sheets", 0)),
                "buy_purity_pct": float(r.get("buy_purity_pct", 0)),
                "persona_tag": str(r.get("主力屬性", "尾盤主力")),
                "action_guide": str(r.get("次日作戰指引", ""))
            })

    # 5. 大盤多空司令速覽
    bull_champion = df_market_kings[df_market_kings["多空陣營"].str.contains("多頭")].iloc[0] if not df_market_kings.empty else None
    bear_champion = df_market_kings[df_market_kings["多空陣營"].str.contains("空頭")].iloc[0] if not df_market_kings.empty else None

    summary_row = [{
        "trade_date": actual_date,
        "bull_champion_broker": str(bull_champion["券商分點"]) if bull_champion is not None else "",
        "bull_champion_amt": float(bull_champion["net_amt_yi"]) if bull_champion is not None else 0,
        "bull_champion_stocks": str(bull_champion["核心標的"]) if bull_champion is not None else "",
        "bear_champion_broker": str(bear_champion["券商分點"]) if bear_champion is not None else "",
        "bear_champion_amt": float(bear_champion["net_amt_yi"]) if bear_champion is not None else 0,
        "bear_champion_stocks": str(bear_champion["核心標的"]) if bear_champion is not None else "",
        "market_sentiment": "偏多震盪" if (bull_champion is not None and bull_champion["net_amt_yi"] > 30) else "中性整理",
        "total_signals_count": len(accum_rows) + len(exit_rows) + len(inst_rows) + len(vwap_rows)
    }]

    payload = {
        "trade_date": actual_date,
        "daily_chip_summary": clean_nan_and_inf(summary_row),
        "chip_accumulation_signals": clean_nan_and_inf(accum_rows),
        "chip_exit_signals": clean_nan_and_inf(exit_rows),
        "broker_institution_ranks": clean_nan_and_inf(inst_rows),
        "vwap_attribution_signals": clean_nan_and_inf(vwap_rows)
    }

    return payload


def sync_single_day(data_dir: str, target_date: str, supabase_url: str, supabase_key: str, dry_run: bool = False) -> bool:
    """同步單一交易日"""
    payload = prepare_chip_payloads(data_dir, target_date)
    actual_date = payload["trade_date"]

    print(f"📊 【{actual_date}】吸籌: {len(payload['chip_accumulation_signals'])} | 出貨: {len(payload['chip_exit_signals'])} | 法人: {len(payload['broker_institution_ranks'])} | VWAP: {len(payload['vwap_attribution_signals'])}")

    out_file = os.path.join(os.path.dirname(__file__), "output", f"mystock_payload_{actual_date}.json")
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    if dry_run or not (supabase_url and supabase_key):
        return True

    success = True
    success &= upsert_to_supabase(supabase_url, supabase_key, "daily_chip_summary", payload["daily_chip_summary"], on_conflict="trade_date")
    success &= upsert_to_supabase(supabase_url, supabase_key, "chip_accumulation_signals", payload["chip_accumulation_signals"], on_conflict="trade_date,period_days,symbol,broker_name")
    success &= upsert_to_supabase(supabase_url, supabase_key, "chip_exit_signals", payload["chip_exit_signals"], on_conflict="trade_date,exit_type,symbol,dump_broker_name")
    success &= upsert_to_supabase(supabase_url, supabase_key, "broker_institution_ranks", payload["broker_institution_ranks"], on_conflict="trade_date,category,broker_name,symbol")
    success &= upsert_to_supabase(supabase_url, supabase_key, "vwap_attribution_signals", payload["vwap_attribution_signals"], on_conflict="trade_date,symbol,broker_name")

    return success


def main():
    parser = argparse.ArgumentParser(description="myStock 雲端籌碼戰情室資料同步模組")
    parser.add_argument("--data-dir", default=r"d:\MyProject\stock_data_analysis\20260822分點資料", help="資料目錄")
    parser.add_argument("--date", default=None, help="指定單一交易日 (YYYY-MM-DD)")
    parser.add_argument("--start-date", default=None, help="批次同步起始日 (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="批次同步結束日 (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="演練模式 (產製並檢驗 JSON Payload，不寫入資料庫)")
    parser.add_argument("--supabase-url", default=None, help="Supabase 專案 URL")
    parser.add_argument("--supabase-key", default=None, help="Supabase API 金鑰 (Service Role 或 Anon)")

    args = parser.parse_args()

    supabase_url = args.supabase_url or os.getenv("SUPABASE_URL")
    supabase_key = args.supabase_key or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    # 批次同步模式
    if args.start_date:
        absr1_files, _ = find_local_files(args.data_dir)
        all_dates = sorted(list(set(extract_date_from_filename(os.path.basename(f)) for f in absr1_files)))
        target_dates = [d for d in all_dates if d >= args.start_date and (not args.end_date or d <= args.end_date)]

        print("\n" + "=" * 65)
        print(f"🚀 啟動歷史批次同步模式：共有 {len(target_dates)} 個交易日 ({target_dates[0]} ~ {target_dates[-1]})")
        print("=" * 65)

        total_ok = 0
        for idx, t_date in enumerate(target_dates):
            print(f"\n[{idx+1}/{len(target_dates)}] 正在處理歷史交易日: {t_date}...")
            ok = sync_single_day(args.data_dir, t_date, supabase_url, supabase_key, args.dry_run)
            if ok:
                total_ok += 1

        print("\n" + "★" * 65)
        print(f"[🎉] 歷史批次同步全數完成！成功處理 {total_ok}/{len(target_dates)} 個交易日。")
        print("★" * 65)

        if not args.dry_run and supabase_url and supabase_key:
            tg_msg = (
                f"🏛️ *myStock 歷史籌碼戰情回補完成*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📅 涵蓋區間：`{target_dates[0]}` ~ `{target_dates[-1]}`\n"
                f"📊 回補天數：`共 {total_ok} 個交易日`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👉 [點此在手機開啟戰情室](https://ark945-mystock.hf.space)"
            )
            send_telegram_notify(tg_msg)
        return

    # 單日模式
    target_date = args.date
    if not target_date:
        absr1_files, _ = find_local_files(args.data_dir)
        target_date = extract_date_from_filename(os.path.basename(absr1_files[-1]))

    success = sync_single_day(args.data_dir, target_date, supabase_url, supabase_key, args.dry_run)
    if success and not args.dry_run and supabase_url and supabase_key:
        print("\n" + "★" * 65)
        print(f"[🎉] 成功將 {target_date} 全市場主力籌碼情報同步至 myStock 戰情室！")
        print("★" * 65)
        tg_msg = (
            f"🏛️ *myStock 雲端籌碼戰情室已同步更新 ({target_date})*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👉 [點此在手機開啟戰情室](https://ark945-mystock.hf.space)"
        )
        send_telegram_notify(tg_msg)


if __name__ == "__main__":
    main()
