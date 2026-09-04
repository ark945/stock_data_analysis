# -*- coding: utf-8 -*-
"""
台股籌碼衍生指標量化分析引擎 (Chip Derivatives & Macro Engine)
============================================================
核心功能：
1. 每日券商買賣家數差 (Broker Count Divergence)：
   - 運算每檔個股「買進券商家數」與「賣出券商家數」之差額。
   - 負值極大 (家數大減)：籌碼高度流向少數主力（極度集中）。
   - 正值極大 (家數大增)：籌碼從少數主力倒向全台散戶（發散出貨）。
2. 資券軋空與接刀坑分析 (Margin & Short Divergence)：
   - 結合主力分點重押與融券暴增 ➔ 🚀 極品軋空主升段。
   - 結合主力出貨與融資暴增 ➔ 🩸 散戶接刀套牢坑。
3. 集保千張大戶雙重共振 (TDCC Shareholder Harmony)：
   - 檢驗標的之「千張大戶持股比率 (%)」與散戶比例，標註籌碼沉澱基期。
4. 大盤微觀期權避震雷達 (Macro Futures Radar)：
   - 結合外資大台 OI 與散戶小台多空比，提供大盤極限情緒警報。
5. 歷史回溯分析：
   - 支援 `--start-date` 與 `--end-date`，回溯 2026-06-01 起任一交易日之衍生指標。
"""

import os
import sys
import glob
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
import pandas as pd
import numpy as np
import duckdb

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def get_stock_name_map(workspace_dir: str = ".") -> Dict[str, str]:
    """讀取股票代號名稱對照表"""
    for p in [
        os.path.join(workspace_dir, "stock_name_map.json"),
        os.path.join(workspace_dir, "..", "stock_data_downloader", "stock_name_map.json"),
        os.path.join(workspace_dir, "output", "stock_name_map.json")
    ]:
        if os.path.exists(p):
            try:
                import json
                with open(p, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
    return {}


def calc_broker_divergence(trade_date: str, broker_file: str, min_sheets: float = 500.0) -> pd.DataFrame:
    """
    使用 DuckDB 計算單日全市場個股券商買賣家數差
    """
    if not os.path.exists(broker_file):
        print(f"[!] 查無分點資料檔: {broker_file}")
        return pd.DataFrame()

    sql = f"""
    SELECT 
        symbol,
        COUNT(DISTINCT CASE WHEN buy_vol > 0 THEN broker_id END) AS buy_brokers,
        COUNT(DISTINCT CASE WHEN sell_vol > 0 THEN broker_id END) AS sell_brokers,
        (COUNT(DISTINCT CASE WHEN buy_vol > 0 THEN broker_id END) - COUNT(DISTINCT CASE WHEN sell_vol > 0 THEN broker_id END)) AS broker_diff,
        ROUND(SUM(buy_vol) / 1000.0, 0) AS total_vol_sheets,
        ROUND(SUM(buy_amt) / 100000.0, 2) AS total_amt_yi
    FROM read_parquet('{broker_file}')
    WHERE symbol NOT LIKE '00%' 
      AND symbol NOT IN ('ZZZZ', 'REG99', 'OTC99', 'Y9999', '1100', '1200')
      AND LENGTH(symbol) <= 5
    GROUP BY symbol
    HAVING total_vol_sheets >= {min_sheets}
    ORDER BY broker_diff ASC
    """
    try:
        df = duckdb.query(sql).df()
        df["trade_date"] = trade_date
        return df
    except Exception as e:
        print(f"[!] 計算券商買賣家數差失敗 ({trade_date}): {e}")
        return pd.DataFrame()


def load_close_data(trade_date: str, broker_dir: str = "./20260822分點資料") -> pd.DataFrame:
    """讀取指定交易日之收盤價 Parquet 檔以取得最新收盤價與市場別"""
    search_dirs = [
        "./temp_cache_close",
        "./cloud_data_close",
        broker_dir,
        "./temp_cache_parquet",
        "./cloud_data",
        "./output",
        "../stock_data_downloader/downloads",
        "../stock_data_downloader/output"
    ]
    for d in search_dirs:
        if not os.path.exists(d):
            continue
        p = os.path.join(d, f"api_close1_{trade_date}_{trade_date}.parquet")
        if not os.path.exists(p):
            cands = glob.glob(os.path.join(d, f"api_close1_*{trade_date}*.parquet"))
            if cands:
                p = cands[0]
        if os.path.exists(p):
            try:
                df = pd.read_parquet(p)
                if not df.empty:
                    cols = [c for c in ["symbol", "market", "close", "name"] if c in df.columns]
                    return df[cols].drop_duplicates(subset=["symbol"])
            except Exception:
                pass

    # 智慧備援：若指定交易日收盤價尚未生成，自動引用最新可用前一交易日 (<= trade_date) 之收盤價數據
    for d in search_dirs:
        if not os.path.exists(d):
            continue
        cands = sorted(glob.glob(os.path.join(d, "api_close1_*.parquet")), reverse=True)
        for c in cands:
            dt_str = os.path.basename(c).replace("api_close1_", "").split("_")[0]
            if dt_str <= trade_date:
                try:
                    df = pd.read_parquet(c)
                    if not df.empty and "close" in df.columns:
                        cols = [c_name for c_name in ["symbol", "market", "close", "name"] if c_name in df.columns]
                        return df[cols].drop_duplicates(subset=["symbol"])
                except Exception:
                    pass
    return pd.DataFrame()


def load_margin_data(trade_date: str, margin_dirs: List[str]) -> pd.DataFrame:
    """讀取指定交易日之融資融券 Parquet，若當日官方尚未公布 (通常每晚 21:30~22:00 公布)，智慧回溯引用最近前一交易日數據"""
    filename = f"api_margin_{trade_date}_{trade_date}.parquet"
    for d in margin_dirs:
        p = os.path.join(d, filename)
        if os.path.exists(p):
            try:
                return pd.read_parquet(p)
            except Exception as e:
                print(f"[!] 讀取融資券失敗 ({p}): {e}")
    # 智慧備援：若當日資券尚未公布，自動引用最新前一交易日 (<= trade_date) 之融資券
    for d in margin_dirs:
        cands = sorted(glob.glob(os.path.join(d, "api_margin_*.parquet")), reverse=True)
        for c in cands:
            dt_str = os.path.basename(c).replace("api_margin_", "").split("_")[0]
            if dt_str <= trade_date:
                try:
                    df = pd.read_parquet(c)
                    if not df.empty:
                        print(f"[*] 提示：{trade_date} 當日融資券官方尚未結算公布，已智慧引用最新基準日 ({dt_str}) 之融資融券數據")
                        return df
                except Exception:
                    pass
    return pd.DataFrame()


def load_tdcc_data(trade_date: str, tdcc_dirs: List[str]) -> pd.DataFrame:
    """讀取最接近指定交易日之集保千張大戶 Parquet"""
    for d in tdcc_dirs:
        files = sorted(glob.glob(os.path.join(d, "api_tdcc_*.parquet")), reverse=True)
        if files:
            # 優先找小於等於 trade_date 的最近週五
            for f in files:
                dt_str = os.path.basename(f).replace("api_tdcc_", "").split("_")[0]
                if dt_str <= trade_date:
                    try:
                        return pd.read_parquet(f)
                    except Exception:
                        pass
            # 備援回傳第一個
            try:
                return pd.read_parquet(files[0])
            except Exception:
                pass
    return pd.DataFrame()


def load_taifex_data(trade_date: str, taifex_dirs: List[str]) -> Optional[Dict[str, Any]]:
    """讀取期交所期貨籌碼指標"""
    filename = f"api_taifex_{trade_date}_{trade_date}.parquet"
    for d in taifex_dirs:
        p = os.path.join(d, filename)
        if os.path.exists(p):
            try:
                df = pd.read_parquet(p)
                if not df.empty:
                    return df.iloc[0].to_dict()
            except Exception:
                pass
    return None


def run_derivatives_analysis_for_date(
    trade_date: str,
    broker_dir: str = "./20260822分點資料",
    margin_dirs: Optional[List[str]] = None,
    tdcc_dirs: Optional[List[str]] = None,
    taifex_dirs: Optional[List[str]] = None,
    output_dir: str = "./daily_reports",
    top_n: int = 30
) -> Dict[str, pd.DataFrame]:
    """執行單一交易日之全套衍生籌碼指標運算"""
    os.makedirs(output_dir, exist_ok=True)
    stock_map = get_stock_name_map()

    if margin_dirs is None:
        margin_dirs = [
            "./output_margin",
            "../stock_data_downloader/output_margin",
            "./data/margin"
        ]
    if tdcc_dirs is None:
        tdcc_dirs = [
            "./output_tdcc",
            "../stock_data_downloader/output_tdcc",
            "./data/tdcc"
        ]
    if taifex_dirs is None:
        taifex_dirs = [
            "./output_taifex",
            "../stock_data_downloader/output_taifex",
            "./data/taifex"
        ]

    broker_file = os.path.join(broker_dir, f"api_absr1_{trade_date}_{trade_date}.parquet")
    if not os.path.exists(broker_file):
        # 嘗試尋找任何包含該日期的分點檔，優先匹配 api_absr1
        cands = glob.glob(os.path.join(broker_dir, f"api_absr1_*{trade_date}*.parquet"))
        if not cands:
            cands = [f for f in glob.glob(os.path.join(broker_dir, f"*{trade_date}*.parquet")) if "margin" not in os.path.basename(f).lower() and "taifex" not in os.path.basename(f).lower() and "tdcc" not in os.path.basename(f).lower()]
        if cands:
            broker_file = cands[0]
        else:
            print(f"[!] 找不到 {trade_date} 的分點檔案")
            return {}

    print("=" * 65)
    print(f"📊 執行台股籌碼衍生量化分析 (基準日: {trade_date})")
    print("=" * 65)

    # 1. 買賣家數差
    print("[1/4] 運算每日券商買賣家數差與集中度...")
    df_div = calc_broker_divergence(trade_date, broker_file)
    if not df_div.empty:
        df_div["stock_name"] = df_div["symbol"].map(lambda s: stock_map.get(str(s), "未知"))
        df_div["name"] = df_div["stock_name"]
        df_div["diff_broker_count"] = df_div["broker_diff"]
        
        # 籌碼高度集中 TOP (負值最大)
        df_concentrated = df_div[df_div["broker_diff"] < 0].sort_values(by="broker_diff", ascending=True).head(top_n).copy()
        df_concentrated["特徵標籤"] = "🔥 籌碼極度集中 (散戶全倒給少數大戶)"
        
        # 籌碼高度發散 TOP (正值最大)
        df_dispersed = df_div[df_div["broker_diff"] > 0].sort_values(by="broker_diff", ascending=False).head(top_n).copy()
        df_dispersed["特徵標籤"] = "⚠️ 籌碼發散出貨 (主力倒貨給散戶)"
    else:
        df_concentrated = pd.DataFrame()
        df_dispersed = pd.DataFrame()

    # 2. 資券軋空與套牢分析
    print("[2/4] 運算融資融券、券資比與軋空/接刀特徵...")
    df_margin = load_margin_data(trade_date, margin_dirs)
    df_squeeze = pd.DataFrame()
    df_trap = pd.DataFrame()

    if not df_margin.empty:
        # 為 concentrated 與 dispersed 補齊收盤價與資券指標
        margin_cols = [c for c in ["symbol", "market", "close", "short_margin_ratio_pct", "margin_net", "short_net", "name"] if c in df_margin.columns]
        if not df_concentrated.empty:
            df_concentrated = pd.merge(df_concentrated, df_margin[margin_cols], on="symbol", how="left", suffixes=("", "_margin"))
            if "name_margin" in df_concentrated.columns:
                df_concentrated["name"] = df_concentrated["name_margin"].fillna(df_concentrated["stock_name"])
                df_concentrated["stock_name"] = df_concentrated["name"]
        if not df_dispersed.empty:
            df_dispersed = pd.merge(df_dispersed, df_margin[margin_cols], on="symbol", how="left", suffixes=("", "_margin"))
            if "name_margin" in df_dispersed.columns:
                df_dispersed["name"] = df_dispersed["name_margin"].fillna(df_dispersed["stock_name"])
                df_dispersed["stock_name"] = df_dispersed["name"]

        if not df_div.empty:
            m_merged = pd.merge(df_margin, df_div[["symbol", "broker_diff", "total_amt_yi"]], on="symbol", how="inner")
            m_merged["stock_name"] = m_merged["name"]
            m_merged["diff_broker_count"] = m_merged["broker_diff"]
            # 極品軋空：券資比 > 10% 且 融券增減 > 20 張 且 家數差為負 (集中)
            sq_mask = (m_merged["short_margin_ratio_pct"] >= 10.0) & (m_merged["short_net"] >= 20) & (m_merged["broker_diff"] < 0)
            df_squeeze = m_merged[sq_mask].sort_values(by=["short_margin_ratio_pct", "short_net"], ascending=[False, False]).head(top_n).copy()
            if not df_squeeze.empty:
                df_squeeze["特徵標籤"] = "🚀 極品軋空候選 (高券資比+融券暴增+大戶收籌)"

            # 散戶接刀坑：融資暴增 > 150 張 且 家數差為正 (發散)
            trap_mask = (m_merged["margin_net"] >= 150) & (m_merged["broker_diff"] > 0)
            df_trap = m_merged[trap_mask].sort_values(by="margin_net", ascending=False).head(top_n).copy()
            if not df_trap.empty:
                df_trap["特徵標籤"] = "🩸 散戶接刀套牢坑 (融資大增+籌碼凌亂散發)"

    # 2.5 載入當日收盤價補全各表之收盤價與市場別
    df_close = load_close_data(trade_date, broker_dir)
    if not df_close.empty:
        def _attach_close(df_in: pd.DataFrame) -> pd.DataFrame:
            if df_in.empty:
                return df_in
            m = pd.merge(df_in, df_close[["symbol", "close", "market", "name"]], on="symbol", how="left", suffixes=("", "_close"))
            if "close_close" in m.columns:
                m["close"] = m["close_close"].fillna(m.get("close", np.nan))
            if "market_close" in m.columns:
                m["market"] = m["market_close"].fillna(m.get("market", "上市"))
            if "name_close" in m.columns:
                cur_name = m["stock_name"] if "stock_name" in m.columns else m.get("name", "")
                m["stock_name"] = cur_name.replace("未知", np.nan).fillna(m["name_close"]).fillna(m["symbol"])
                m["name"] = m["stock_name"]
            return m

        df_concentrated = _attach_close(df_concentrated)
        df_dispersed = _attach_close(df_dispersed)
        df_squeeze = _attach_close(df_squeeze)
        df_trap = _attach_close(df_trap)

    # 2.6 使用全市場對照表補全任何仍為空值的市場別 (杜絕 nan 標籤)
    try:
        from find_similar_cases import get_stock_market_map
        all_market_map = get_stock_market_map()
        for df_target in [df_concentrated, df_dispersed, df_squeeze, df_trap]:
            if not df_target.empty:
                if "market" not in df_target.columns:
                    df_target["market"] = df_target["symbol"].map(all_market_map).fillna("上市")
                else:
                    df_target["market"] = df_target["market"].fillna(df_target["symbol"].map(all_market_map)).fillna("上市")
                    df_target["market"] = df_target["market"].replace({"nan": "上櫃", "None": "上櫃", np.nan: "上櫃"})
    except Exception:
        pass

    # 3. 集保千張大戶檢驗
    print("[3/4] 比對集保千張大戶鎖碼比例...")
    df_tdcc = load_tdcc_data(trade_date, tdcc_dirs)
    if not df_tdcc.empty:
        tdcc_cols = [c for c in ["symbol", "large_shareholder_pct", "retail_shareholder_pct", "total_shareholders"] if c in df_tdcc.columns]
        for df_target in [df_concentrated, df_dispersed, df_squeeze, df_trap]:
            if not df_target.empty and "symbol" in df_target.columns:
                existing_drop = [c for c in ["large_shareholder_pct", "retail_shareholder_pct", "total_shareholders", "大戶鎖碼等級"] if c in df_target.columns]
                if existing_drop:
                    df_target.drop(columns=existing_drop, inplace=True)
                merged = pd.merge(df_target, df_tdcc[tdcc_cols], on="symbol", how="left")
                for c in ["large_shareholder_pct", "retail_shareholder_pct", "total_shareholders"]:
                    if c in merged.columns:
                        df_target[c] = merged[c].values
                if "large_shareholder_pct" in df_target.columns:
                    df_target["大戶鎖碼等級"] = df_target["large_shareholder_pct"].apply(
                        lambda x: "👑 超高鎖碼 (>70%)" if pd.notna(x) and x >= 70 else ("🔒 穩健鎖碼 (>50%)" if pd.notna(x) and x >= 50 else "一般 (<50%)")
                    )

    # 4. 大盤期權避震雷達
    print("[4/4] 提取期交所微觀期貨指標...")
    taifex_info = load_taifex_data(trade_date, taifex_dirs)
    macro_df = pd.DataFrame([taifex_info]) if taifex_info else pd.DataFrame()

    # 匯出至 Excel 多工作表報表
    out_excel = os.path.join(output_dir, f"籌碼衍生指標戰報_{trade_date}.xlsx")
    with pd.ExcelWriter(out_excel, engine="openpyxl") as writer:
        if not df_concentrated.empty:
            df_concentrated.to_excel(writer, sheet_name="籌碼極度集中(買賣家數差)", index=False)
        if not df_dispersed.empty:
            df_dispersed.to_excel(writer, sheet_name="籌碼發散出貨(家數差為正)", index=False)
        if not df_squeeze.empty:
            df_squeeze.to_excel(writer, sheet_name="極品軋空候選", index=False)
        if not df_trap.empty:
            df_trap.to_excel(writer, sheet_name="散戶接刀套牢坑", index=False)
        if not macro_df.empty:
            macro_df.to_excel(writer, sheet_name="大盤微觀期權避震", index=False)

    print(f"[✓] 籌碼衍生指標報表已匯出: {out_excel}")
    if taifex_info:
        print(f"[*] 大盤微觀情緒: {taifex_info.get('macro_sentiment', '無')}")
        print(f"[*] 外資大台淨口數: {taifex_info.get('foreign_tx_oi', 0):+,.0f}口 | 散戶小台多空比: {taifex_info.get('retail_mtx_ratio_pct', 0):+.2f}%")

    return {
        "concentrated": df_concentrated,
        "dispersed": df_dispersed,
        "squeeze": df_squeeze,
        "trap": df_trap,
        "macro": macro_df
    }


def backfill_derivatives_range(start_date: str, end_date: str, broker_dir: str = "./20260822分點資料"):
    """歷史區間批次回補分析"""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    cur_dt = start_dt

    dates = []
    while cur_dt <= end_dt:
        if cur_dt.weekday() < 5:
            d_str = cur_dt.strftime("%Y-%m-%d")
            # 檢查是否有分點檔
            if glob.glob(os.path.join(broker_dir, f"*{d_str}*.parquet")):
                dates.append(d_str)
        cur_dt += timedelta(days=1)

    print("=" * 65)
    print(f"🚀 啟動籌碼衍生指標歷史批次分析 (共 {len(dates)} 個有效交易日)")
    print(f"[*] 區間: {start_date} ~ {end_date}")
    print("=" * 65)

    for idx, d_str in enumerate(dates):
        print(f"\n[{idx+1}/{len(dates)}] 正在分析交易日: {d_str}")
        run_derivatives_analysis_for_date(d_str, broker_dir=broker_dir)

    print("\n" + "★" * 65)
    print(f"[🎉] 全歷史區間籌碼衍生指標分析完成！")
    print("★" * 65)


def main():
    parser = argparse.ArgumentParser(description="台股籌碼衍生指標量化分析引擎")
    parser.add_argument("--date", default="", help="指定單一交易日 (YYYY-MM-DD)")
    parser.add_argument("--start-date", default="", help="批次回補起始日 (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="", help="批次回補結束日 (YYYY-MM-DD，若未指定則為今日)")
    parser.add_argument("--broker-dir", default="./20260822分點資料", help="分點 Parquet 目錄")
    parser.add_argument("--output-dir", default="./daily_reports", help="分析報告匯出目錄")
    parser.add_argument("--top", type=int, default=30, help="每組名單輸出筆數 (預設: 30)")

    args = parser.parse_args()

    today_str = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")

    if args.start_date:
        end_d = args.end_date if args.end_date else today_str
        backfill_derivatives_range(args.start_date, end_d, broker_dir=args.broker_dir)
        return

    target_d = args.date if args.date else today_str
    run_derivatives_analysis_for_date(target_d, broker_dir=args.broker_dir, output_dir=args.output_dir, top_n=args.top)


if __name__ == "__main__":
    main()
