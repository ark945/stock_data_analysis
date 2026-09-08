# -*- coding: utf-8 -*-
"""
雲端量化分析與 HTML 郵件報告生成器 (Cloud Report Generator)
============================================================
功能：
1. 整合 DuckDB 分析引擎，執行「川湖 (2059) + 凱基-三多 (9275)」主力重押模型
2. 自動產出現代 FinTech 高質感響應式 HTML 郵件內容 (含統計指標、表格、標籤)
3. 自動產出完整明細之 Excel (.xlsx) 檔案作為郵件附件
"""

import os
import sys
import glob
import json
import time
import argparse
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any

import duckdb
import pandas as pd
import numpy as np

# 導入名稱對照模組
from find_similar_cases import get_stock_name_map, get_stock_market_map, get_broker_name_map


def run_heavy_accumulation_analysis(
    parquet_files: List[str],
    min_net_amt_yi: float = 0.3,             # 淨買超金額門檻 (億元，預設 3000 萬元 = 0.3 億)
    min_buy_ratio_pct: float = 70.0,         # 買進純度佔比門檻 (預設 70%)
    min_net_vol_sheets: float = 50.0,        # 淨買超張數門檻 (預設 50 張)
    min_trade_days: int = 1,                 # 最小活躍天數
    ignition_threshold_ratio: float = 0.20,  # 主力點火確認門檻比例 (預設 20%)
    exclude_etf: bool = True,                # 是否過濾 ETF 標的 (預設 True，排除 00 開頭被動標的)
    sort_by: str = "score",                  # 排序方式: "score" (吸籌強度評分優先，推薦) 或 "amt" (金額優先)
    top_n: int = 30,
    close_price_files: Optional[List[str]] = None,  # 同期間每日收盤價 Parquet (api_close1_*)，提供則附加回測報酬率與分點集中度
    position_lookback_days: int = 20         # 點火日相對高低位置之回看交易日數 (需 close_price_files 浵蓋此範圍才有效)
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    透過 DuckDB 執行川湖+凱基三多重押模型分析 (含主力點火起算日、吃貨歷時與吸籌強度評分排序)
    若提供 close_price_files，額外附加「點火後報酬率」(訊號回測驗證) 與「分點成交集中度」欄位
    回傳 (篩選結果 DataFrame, 統計數據概覽字典)
    """
    if not parquet_files:
        return pd.DataFrame(), {}

    # 過濾出標準分點彙總檔案 (api_absr1)
    absr1_files = [
        f.replace("\\", "/") for f in parquet_files
        if "finmind" not in os.path.basename(f).lower() and "close1" not in os.path.basename(f).lower()
    ]
    if not absr1_files:
        absr1_files = [f.replace("\\", "/") for f in parquet_files]

    stock_names = get_stock_name_map()
    broker_names = get_broker_name_map()

    extra_sql_filter = " AND symbol NOT IN ('ZZZZ', 'REG99', 'OTC99', 'Y9999')"  # 排除資料來源已知的佔位/雜訊代碼
    if exclude_etf:
        extra_sql_filter += " AND NOT (symbol LIKE '00%')"

    sql = f"""
    WITH raw_trades AS (
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
        FROM read_parquet({absr1_files}, union_by_name=true)
        WHERE (buy_amt >= 100 OR sell_amt >= 100){extra_sql_filter}
    ),
    daily_trades AS (
        SELECT
            symbol,
            broker_id,
            SUBSTRING(trade_date, 1, 10) AS trade_date,
            SUM(buy_vol) AS buy_vol,
            SUM(sell_vol) AS sell_vol,
            SUM(net_vol) AS net_vol,
            SUM(buy_amt) AS buy_amt,
            SUM(sell_amt) AS sell_amt,
            SUM(net_amt) AS net_amt,
            MAX(is_buy_day) AS is_buy_day
        FROM raw_trades
        GROUP BY symbol, broker_id, SUBSTRING(trade_date, 1, 10)
    ),
    daily_cum AS (
        SELECT
            *,
            SUM(net_amt) OVER (
                PARTITION BY symbol, broker_id 
                ORDER BY trade_date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_net_amt
        FROM daily_trades
    ),
    summary_per_pair AS (
        SELECT 
            symbol,
            broker_id,
            MIN(trade_date) AS first_date,
            MAX(trade_date) AS last_date,
            COUNT(DISTINCT trade_date) AS trade_days,
            SUM(is_buy_day) AS buy_days,
            SUM(buy_vol) / 1000.0 AS buy_vol_sheets,
            SUM(sell_vol) / 1000.0 AS sell_vol_sheets,
            SUM(net_vol) / 1000.0 AS net_vol_sheets,
            SUM(buy_amt) / 100000.0 AS buy_amt_yi,
            SUM(sell_amt) / 100000.0 AS sell_amt_yi,
            SUM(net_amt) / 100000.0 AS net_amt_yi,
            SUM(net_amt) AS total_net_amt_k,
            ROUND((SUM(buy_amt) * 1000.0) / NULLIF(SUM(buy_vol), 0), 2) AS buy_avg_price,
            ROUND((SUM(sell_amt) * 1000.0) / NULLIF(SUM(sell_vol), 0), 2) AS sell_avg_price,
            ROUND(SUM(buy_vol) * 100.0 / NULLIF(SUM(buy_vol) + SUM(sell_vol), 0), 1) AS buy_ratio_pct,
            ROUND(SUM(is_buy_day) * 100.0 / NULLIF(COUNT(DISTINCT trade_date), 0), 1) AS buy_day_pct
        FROM daily_trades
        GROUP BY symbol, broker_id
    ),
    ignition_dates AS (
        SELECT
            d.symbol,
            d.broker_id,
            MIN(d.trade_date) AS ignition_date
        FROM daily_cum d
        JOIN summary_per_pair s ON d.symbol = s.symbol AND d.broker_id = s.broker_id
        WHERE (
            -- ★ 核心判定：累計淨買超首度達到該主力總部位的 20% (自適應高低價股與主力規模)
            d.cum_net_amt >= (s.total_net_amt_k * {ignition_threshold_ratio})
        )
        GROUP BY d.symbol, d.broker_id
    )
    SELECT 
        s.symbol,
        s.broker_id,
        s.first_date,
        COALESCE(i.ignition_date, s.first_date) AS ignition_date,
        s.last_date,
        s.trade_days,
        s.buy_days,
        s.buy_day_pct,
        ROUND(s.buy_vol_sheets, 1) AS buy_vol_sheets,
        ROUND(s.sell_vol_sheets, 1) AS sell_vol_sheets,
        ROUND(s.net_vol_sheets, 1) AS net_vol_sheets,
        s.buy_ratio_pct,
        s.buy_avg_price,
        s.sell_avg_price,
        ROUND(s.buy_amt_yi, 2) AS buy_amt_yi,
        ROUND(s.net_amt_yi, 2) AS net_amt_yi
    FROM summary_per_pair s
    LEFT JOIN ignition_dates i ON s.symbol = i.symbol AND s.broker_id = i.broker_id
    WHERE s.net_amt_yi >= {min_net_amt_yi}
      AND s.buy_ratio_pct >= {min_buy_ratio_pct}
      AND s.net_vol_sheets >= {min_net_vol_sheets}
      AND s.trade_days >= {min_trade_days}
    """

    df = duckdb.query(sql).to_df()
    if df.empty:
        return pd.DataFrame(), {"total_records": 0, "unique_stocks": 0}

    # 計算主力吃貨歷時天數 (以日曆天計算，如 2026-07-08 至 2026-08-27 為 51 天)
    df["first_date"] = df["first_date"].astype(str).str.slice(0, 10)
    df["ignition_date"] = df["ignition_date"].astype(str).str.slice(0, 10)
    df["last_date"] = df["last_date"].astype(str).str.slice(0, 10)
    
    ign_dt = pd.to_datetime(df["ignition_date"])
    lst_dt = pd.to_datetime(df["last_date"])
    df["accum_days"] = (lst_dt - ign_dt).dt.days + 1

    # 計算主力吸籌強度評分 (Score 0~100 分，融合資金規模、買進純度與持續吃貨天數)
    amt_score = np.clip(np.log10(np.maximum(1.0, df["net_amt_yi"] * 100000.0)) * 8.0, 0, 35.0)
    ratio_score = np.clip((df["buy_ratio_pct"] - 50.0) * 0.8, 0, 40.0)
    day_score = np.clip(df["buy_day_pct"] * 0.25, 0, 25.0)
    df["score"] = (amt_score + ratio_score + day_score).round(1)

    # 依排序基準排列
    if sort_by == "score":
        df.sort_values(by=["score", "net_amt_yi"], ascending=[False, False], inplace=True)
    else:
        df.sort_values(by=["net_amt_yi", "score"], ascending=[False, False], inplace=True)

    stock_markets = get_stock_market_map()
    df["stock_name"] = df["symbol"].apply(lambda s: stock_names.get(s, ""))
    df["market"] = df["symbol"].apply(lambda s: stock_markets.get(s, "上市" if str(s).isdigit() and int(s) < 3000 else "上櫃"))
    df["broker_name"] = df["broker_id"].apply(lambda b: broker_names.get(b, ""))

    df["股票標的"] = df.apply(lambda r: f"{r['symbol']} {r['stock_name']} ({r['market']})".strip(), axis=1)
    df["主力分點"] = df.apply(lambda r: f"{r['broker_id']} {r['broker_name']}".strip(), axis=1)

    # 附加回測報酬率 (點火日→最新收盤價之漲跌幅) 與分點成交集中度 (需提供同期收盤價檔案)
    win_rate = None
    avg_return_pct = None
    if close_price_files:
        close_files_norm = [f.replace("\\", "/") for f in close_price_files]
        try:
            price_df = duckdb.query(f"""
                SELECT CAST(symbol AS VARCHAR) AS symbol,
                       SUBSTRING(CAST(trade_date AS VARCHAR), 1, 10) AS trade_date,
                       CAST(close AS DOUBLE) AS close,
                       CAST(high AS DOUBLE) AS high,
                       CAST(low AS DOUBLE) AS low,
                       CAST(volume AS DOUBLE) AS volume
                FROM read_parquet({close_files_norm}, union_by_name=true)
                WHERE symbol IS NOT NULL
            """).to_df()
        except Exception as e:
            print(f"[!] DuckDB 批次載入收盤價異常 ({e})，啟動 Arrow/Pandas 安全讀取模式...")
            dfs = []
            for f in close_price_files:
                try:
                    tdf = pd.read_parquet(f, columns=["symbol", "trade_date", "close", "high", "low", "volume"])
                    if not tdf.empty:
                        tdf["symbol"] = tdf["symbol"].astype(str)
                        tdf["trade_date"] = tdf["trade_date"].astype(str).str[:10]
                        for col in ["close", "high", "low", "volume"]:
                            tdf[col] = pd.to_numeric(tdf[col], errors="coerce")
                        dfs.append(tdf)
                except Exception:
                    continue
            price_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        ignition_price = price_df.rename(columns={"trade_date": "ignition_date", "close": "ignition_close"})[["symbol", "ignition_date", "ignition_close"]]
        latest_price = price_df.rename(columns={"trade_date": "last_date", "close": "latest_close"})[["symbol", "last_date", "latest_close"]]
        df = df.merge(ignition_price, on=["symbol", "ignition_date"], how="left")
        df = df.merge(latest_price, on=["symbol", "last_date"], how="left")
        df["return_pct"] = np.where(
            df["ignition_close"].notna() & df["latest_close"].notna() & (df["ignition_close"] > 0),
            ((df["latest_close"] - df["ignition_close"]) / df["ignition_close"] * 100).round(2),
            np.nan
        )

        # 成本偏離度：以主力真實買進均價（而非點火日收盤價）對比最新收盤價，反映主力目前真實損益
        df["cost_deviation_pct"] = np.where(
            df["latest_close"].notna() & df["buy_avg_price"].notna() & (df["buy_avg_price"] > 0),
            ((df["latest_close"] - df["buy_avg_price"]) / df["buy_avg_price"] * 100).round(2),
            np.nan
        )

        # 持有期間最大漲幅/最大回撤：於點火日至最新活躍日區間內，抓取最高/最低價相對點火日收盤價之乖離
        period_pairs = df[["symbol", "ignition_date", "last_date"]].drop_duplicates()
        period_range_df = duckdb.query("""
            SELECT p.symbol, p.ignition_date, p.last_date,
                MAX(c.high) AS period_max_high,
                MIN(c.low) AS period_min_low
            FROM period_pairs p
            JOIN price_df c
              ON p.symbol = c.symbol
             AND c.trade_date BETWEEN p.ignition_date AND p.last_date
            GROUP BY p.symbol, p.ignition_date, p.last_date
        """).to_df()
        df = df.merge(period_range_df, on=["symbol", "ignition_date", "last_date"], how="left")
        df["period_max_gain_pct"] = np.where(
            df["ignition_close"].notna() & df["period_max_high"].notna() & (df["ignition_close"] > 0),
            ((df["period_max_high"] - df["ignition_close"]) / df["ignition_close"] * 100).round(2),
            np.nan
        )
        df["period_max_drawdown_pct"] = np.where(
            df["ignition_close"].notna() & df["period_min_low"].notna() & (df["ignition_close"] > 0),
            ((df["period_min_low"] - df["ignition_close"]) / df["ignition_close"] * 100).round(2),
            np.nan
        )
        df.drop(columns=["period_max_high", "period_min_low"], inplace=True)

        # 點火日相對高低位置：以點火日往前回看 N 交易日窗口內的最高/最低價，判斷點火時是低接還是追價
        position_df = duckdb.query(f"""
            WITH ordered AS (
                SELECT symbol, trade_date,
                    MIN(low) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {position_lookback_days - 1} PRECEDING AND CURRENT ROW
                    ) AS window_min_low,
                    MAX(high) OVER (
                        PARTITION BY symbol ORDER BY trade_date
                        ROWS BETWEEN {position_lookback_days - 1} PRECEDING AND CURRENT ROW
                    ) AS window_max_high
                FROM price_df
            )
            SELECT symbol, trade_date AS ignition_date, window_min_low, window_max_high
            FROM ordered
        """).to_df()
        df = df.merge(position_df, on=["symbol", "ignition_date"], how="left")
        df["position_in_range_pct"] = np.where(
            df["ignition_close"].notna() & df["window_max_high"].notna() & df["window_min_low"].notna() & (df["window_max_high"] > df["window_min_low"]),
            ((df["ignition_close"] - df["window_min_low"]) / (df["window_max_high"] - df["window_min_low"]) * 100).round(1),
            np.nan
        )
        df.drop(columns=["window_min_low", "window_max_high"], inplace=True)

        period_vol = price_df.groupby("symbol")["volume"].sum().rename("period_total_vol").reset_index()
        df = df.merge(period_vol, on="symbol", how="left")
        df["concentration_pct"] = np.where(
            df["period_total_vol"].notna() & (df["period_total_vol"] > 0),
            (df["net_vol_sheets"] * 1000 / df["period_total_vol"] * 100).round(2),
            np.nan
        )
        df.drop(columns=["period_total_vol"], inplace=True)

        valid_return = df["return_pct"].dropna()
        if not valid_return.empty:
            win_rate = round((valid_return > 0).mean() * 100, 1)
            avg_return_pct = round(valid_return.mean(), 2)
    else:
        df["ignition_close"] = np.nan
        df["latest_close"] = np.nan
        df["return_pct"] = np.nan
        df["cost_deviation_pct"] = np.nan
        df["period_max_gain_pct"] = np.nan
        df["period_max_drawdown_pct"] = np.nan
        df["position_in_range_pct"] = np.nan
        df["concentration_pct"] = np.nan

    # 摘要統計
    summary = {
        "scan_files_count": len(absr1_files),
        "start_date": df["first_date"].min(),
        "end_date": df["last_date"].max(),
        "total_targets": len(df),
        "unique_stocks": df["symbol"].nunique(),
        "top_stock": df.iloc[0]["股票標的"] if not df.empty else "無",
        "top_broker": df.iloc[0]["主力分點"] if not df.empty else "無",
        "top_amt_yi": float(df.iloc[0]["net_amt_yi"]) if not df.empty else 0.0,
        "total_heavy_amt_yi": round(df["net_amt_yi"].sum(), 2),
        "backtest_win_rate": win_rate,
        "backtest_avg_return_pct": avg_return_pct
    }

    return df, summary


def enrich_cross_period_momentum(
    df_5d: pd.DataFrame,
    df_10d: pd.DataFrame,
    df_20d: Optional[pd.DataFrame] = None
) -> None:
    """
    跨週期主力動能加速度分析 (Cross-Period Momentum Intelligence)
    以純記憶體向量化比對 5日 vs 10日 淨買超佔比與推進節奏，直接豐富化 DataFrame：
    - momentum_tag: 徽章名稱 (如 '🚀 突發急行軍 (100%)', '🌊 勻速波段建倉 (52%)', '⚠️ 買盤已熄火 (12%)', '⚡ 游資短點火')
    - momentum_ratio_pct: 5d淨買超 / 10d淨買超 百分比
    - action_guide: 針對動能型態之專屬次日實戰作戰指引
    """
    if df_5d is None or df_10d is None:
        return

    net_10d_map = {}
    if not df_10d.empty:
        for _, r in df_10d.iterrows():
            k = (str(r.get("symbol", "")), str(r.get("broker_id", "")))
            net_10d_map[k] = float(r.get("net_amt_yi", 0))

    net_5d_map = {}
    if not df_5d.empty:
        for _, r in df_5d.iterrows():
            k = (str(r.get("symbol", "")), str(r.get("broker_id", "")))
            net_5d_map[k] = float(r.get("net_amt_yi", 0))

    # 1. 豐富化 5 日 DataFrame
    if not df_5d.empty:
        m_tags_5d = []
        ratios_5d = []
        guides_5d = []
        for _, r in df_5d.iterrows():
            k = (str(r.get("symbol", "")), str(r.get("broker_id", "")))
            amt_5d = float(r.get("net_amt_yi", 0))
            dev = float(r["cost_deviation_pct"]) if pd.notna(r.get("cost_deviation_pct")) else 0.0

            if k in net_10d_map and net_10d_map[k] > 0:
                amt_10d = net_10d_map[k]
                ratio = round((amt_5d / amt_10d) * 100, 1)
                ratios_5d.append(ratio)
                if ratio >= 80.0:
                    # 結合成本偏離度防護濾網 (防範高檔竭盡買盤 Buy Climax)
                    if dev > 12.0:
                        m_tags_5d.append(f"🚨 高檔乖離急行軍 ({ratio:.0f}%)")
                        guides_5d.append(f"短線急行軍暴買但現價已偏離主力成本 +{dev:.1f}%，慎防高檔竭盡買盤或誘多拉高，切忌盲目追高，提防爆量長黑")
                    elif dev < -5.0:
                        m_tags_5d.append(f"💎 破發逆勢急行軍 ({ratio:.0f}%)")
                        guides_5d.append(f"主力於成本下方 ({dev:.1f}%) 逆勢暴買急行軍，護盤反彈意圖濃厚，具高性價比安全防守點位")
                    else:
                        m_tags_5d.append(f"🚀 突破急行軍 ({ratio:.0f}%)")
                        guides_5d.append(f"主力於成本附近 (乖離 {dev:+.1f}%) 發動暴買急行軍，時效爆發力極強，可沿主力加權成本防守布局")
                elif 40.0 <= ratio < 80.0:
                    if dev > 15.0:
                        m_tags_5d.append(f"🌊 波段高檔推升 ({ratio:.0f}%)")
                        guides_5d.append(f"主力雙週內勻速吃貨，但現價已偏離成本 +{dev:.1f}%，持股者沿均線移動停利，空手者勿市價追高")
                    else:
                        m_tags_5d.append(f"🌊 勻速波段建倉 ({ratio:.0f}%)")
                        guides_5d.append("主力雙週內有紀律持續吃貨，籌碼結構健康穩定，適合沿均線波段順勢持有")
                elif ratio <= 20.0:
                    m_tags_5d.append(f"⚠️ 買盤已熄火 ({ratio:.0f}%)")
                    guides_5d.append("10日總額雖高，但近5日買盤近乎停滯，慎防主力吃飽收手，切忌盲目追高")
                else:
                    m_tags_5d.append(f"⏳ 買盤放緩 ({ratio:.0f}%)")
                    guides_5d.append("主力吃貨節奏有所放緩，建議觀察下檔均線支撐強度")
            else:
                ratios_5d.append(100.0)
                if dev > 10.0:
                    m_tags_5d.append("⚡ 游資高檔搶短")
                    guides_5d.append(f"前段無底倉且偏離均價 +{dev:.1f}%，短線熱錢或隔日沖高檔搶短，隔日極易開高反手倒貨，嚴格落實停利")
                else:
                    m_tags_5d.append("⚡ 游資短點火")
                    guides_5d.append("前段無長莊底倉，短線熱錢或隔日沖快速點火，宜設嚴格移動停利")

        df_5d["momentum_tag"] = m_tags_5d
        df_5d["momentum_ratio_pct"] = ratios_5d
        df_5d["action_guide"] = guides_5d

    # 2. 豐富化 10 日 DataFrame
    if not df_10d.empty:
        m_tags_10d = []
        ratios_10d = []
        guides_10d = []
        for _, r in df_10d.iterrows():
            k = (str(r.get("symbol", "")), str(r.get("broker_id", "")))
            amt_10d = float(r.get("net_amt_yi", 0))
            dev = float(r["cost_deviation_pct"]) if pd.notna(r.get("cost_deviation_pct")) else 0.0

            if k in net_5d_map and amt_10d > 0:
                amt_5d = net_5d_map[k]
                ratio = round((amt_5d / amt_10d) * 100, 1)
                ratios_10d.append(ratio)
                if ratio >= 80.0:
                    if dev > 12.0:
                        m_tags_10d.append(f"🚨 高檔乖離急行軍 (近5d佔{ratio:.0f}%)")
                        guides_10d.append(f"雙週買盤全在近2~3天狂砸但偏離成本 +{dev:.1f}%，慎防拉高竭盡，嚴格落實分批停利")
                    else:
                        m_tags_10d.append(f"🚀 突破急行軍 (近5d佔{ratio:.0f}%)")
                        guides_10d.append(f"雙週買盤全在近2~3天狂砸突破 (乖離 {dev:+.1f}%)，剛點火爆發力強，回測均線支撐布局")
                elif 40.0 <= ratio < 80.0:
                    m_tags_10d.append(f"🌊 勻速波段吃貨 (近5d佔{ratio:.0f}%)")
                    guides_10d.append("主力雙週每天持續買超，波段籌碼沉澱紮實，適合中線波段抱牢")
                elif ratio <= 20.0:
                    m_tags_10d.append(f"⚠️ 買盤已熄火 (近5d佔{ratio:.0f}%)")
                    guides_10d.append("雙週總量看似龐大，但近5日已停滯，主力吃飽待散戶抬轎，嚴禁追高")
                else:
                    m_tags_10d.append(f"⏳ 節奏放緩 (近5d佔{ratio:.0f}%)")
                    guides_10d.append("主力買盤節奏稍緩，留意整理區間支撐")
            else:
                ratios_10d.append(0.0)
                m_tags_10d.append("❄️ 近期停滯/退場")
                guides_10d.append("主力近5日完全停手或轉調節，前段買盤已鈍化，宜保守觀望")

        df_10d["momentum_tag"] = m_tags_10d
        df_10d["momentum_ratio_pct"] = ratios_10d
        df_10d["action_guide"] = guides_10d


def generate_whale_matrix_html_section(
    df_5d: pd.DataFrame,
    df_10d: Optional[pd.DataFrame] = None,
    df_20d: Optional[pd.DataFrame] = None,
    files_10d: Optional[List[str]] = None,
    files_20d: Optional[List[str]] = None,
    top_n: int = 5
) -> str:
    """
    生成【🐳 權值巨鯨籌碼追蹤矩陣 (Whale Matrix)】HTML 區塊
    以近 5 日淨買超金額 (net_amt_yi) 降冪排序，並排穿透 5d(點火)、10d(雙週)、20d(底倉) 三週期之資金佈局。
    """
    if df_5d is None or df_5d.empty:
        return ""

    candidates = df_5d.sort_values(by="net_amt_yi", ascending=False)
    whale_df = candidates.head(top_n).copy()
    if whale_df.empty or float(whale_df.iloc[0].get("net_amt_yi", 0)) < 1.0:
        return ""

    map_10d = {}
    if df_10d is not None and not df_10d.empty:
        for _, r in df_10d.iterrows():
            k = (str(r.get("symbol", "")), str(r.get("broker_id", "")))
            map_10d[k] = float(r.get("net_amt_yi", 0))

    map_20d = {}
    if df_20d is not None and not df_20d.empty:
        for _, r in df_20d.iterrows():
            k = (str(r.get("symbol", "")), str(r.get("broker_id", "")))
            map_20d[k] = float(r.get("net_amt_yi", 0))

    # 若特定巨鯨在 10d/20d 因純度未達 70% 沒入榜，直接向底層 Parquet 提取真實金額 (零盲點)
    missing_10d_pairs = []
    missing_20d_pairs = []
    for _, r in whale_df.iterrows():
        pair = (str(r.get("symbol", "")), str(r.get("broker_id", "")))
        if pair not in map_10d:
            missing_10d_pairs.append(pair)
        if pair not in map_20d:
            missing_20d_pairs.append(pair)

    if files_10d and missing_10d_pairs:
        try:
            f_norm = [f.replace("\\", "/") for f in files_10d]
            where_clauses = " OR ".join([f"(symbol = '{s}' AND broker_id = '{b}')" for s, b in missing_10d_pairs])
            sql_patch_10 = f"SELECT symbol, broker_id, ROUND(SUM(net_amt) / 100000.0, 2) as net_amt_yi FROM read_parquet({f_norm}) WHERE {where_clauses} GROUP BY symbol, broker_id"
            df_patch_10 = duckdb.query(sql_patch_10).to_df()
            for _, pr in df_patch_10.iterrows():
                map_10d[(str(pr["symbol"]), str(pr["broker_id"]))] = float(pr["net_amt_yi"])
        except Exception as _e:
            pass

    if files_20d and missing_20d_pairs:
        try:
            f_norm = [f.replace("\\", "/") for f in files_20d]
            where_clauses = " OR ".join([f"(symbol = '{s}' AND broker_id = '{b}')" for s, b in missing_20d_pairs])
            sql_patch_20 = f"SELECT symbol, broker_id, ROUND(SUM(net_amt) / 100000.0, 2) as net_amt_yi FROM read_parquet({f_norm}) WHERE {where_clauses} GROUP BY symbol, broker_id"
            df_patch_20 = duckdb.query(sql_patch_20).to_df()
            for _, pr in df_patch_20.iterrows():
                map_20d[(str(pr["symbol"]), str(pr["broker_id"]))] = float(pr["net_amt_yi"])
        except Exception as _e:
            pass

    rows_html = ""
    for idx, (_, row) in enumerate(whale_df.iterrows()):
        rank = idx + 1
        rank_badge_bg = "#dc2626" if rank <= 3 else "#2563eb"
        sym = str(row.get("symbol", ""))
        sname = str(row.get("stock_name", ""))
        bname = str(row.get("主力分點", row.get("broker_name", "")))
        bid = str(row.get("broker_id", ""))
        m_type = str(row.get("market", "上市"))

        amt_5d = float(row.get("net_amt_yi", 0))
        amt_10d = map_10d.get((sym, bid), None)
        amt_20d = map_20d.get((sym, bid), None)

        amt_10d_str = f"+{amt_10d:,.2f} 億" if (amt_10d is not None and amt_10d > 0) else ("-" if amt_10d is None else f"{amt_10d:,.2f} 億")
        amt_20d_str = f"+{amt_20d:,.2f} 億" if (amt_20d is not None and amt_20d > 0) else ("-" if amt_20d is None else f"{amt_20d:,.2f} 億")

        m_tag = str(row.get("momentum_tag", ""))
        if not m_tag or m_tag == "None":
            if amt_10d and amt_10d > 0:
                pct = (amt_5d / amt_10d) * 100
                m_tag = f"🚀 急行軍 ({pct:.0f}%)" if pct >= 80 else (f"🌊 勻速波段 ({pct:.0f}%)" if pct >= 40 else f"⏳ 放緩 ({pct:.0f}%)")
            else:
                m_tag = "⚡ 突發點火"

        if amt_20d and amt_20d >= 50.0 and amt_5d >= 10.0:
            strategy = "🏰 長莊二次總攻 (長波底倉渾厚，短線爆發力極強)"
            strat_color = "#7c3aed"
        elif amt_20d and amt_20d >= 20.0 and (amt_5d / amt_20d) <= 0.6:
            strategy = "💎 月線波段定海神針 (籌碼高度鎖定，沿均線順勢持有)"
            strat_color = "#0369a1"
        elif "急行軍" in m_tag or (amt_10d and (amt_5d / amt_10d) >= 0.85):
            strategy = "🚀 短線瘋狂點火 (突破前夕急行軍，時效爆發力極高)"
            strat_color = "#b91c1c"
        else:
            strategy = "🌊 穩健加碼佈局 (主力持續有節奏建倉)"
            strat_color = "#047857"

        m_badge = '<span style="background-color: #e6f7ff; color: #096dd9; border: 1px solid #91d5ff; font-size: 11px; padding: 1px 4px; border-radius: 3px; font-weight: bold; margin-left: 4px;">上市</span>' if m_type == "上市" else '<span style="background-color: #f6ffed; color: #389e0d; border: 1px solid #b7eb8f; font-size: 11px; padding: 1px 4px; border-radius: 3px; font-weight: bold; margin-left: 4px;">上櫃</span>'

        rows_html += f"""
        <tr style="border-bottom: 1px solid #f1f5f9; font-size: 13px;">
            <td style="padding: 10px 8px; text-align: center; white-space: nowrap;">
                <span style="background-color: {rank_badge_bg}; color: #ffffff; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: bold;">{rank}</span>
            </td>
            <td style="padding: 10px; font-weight: bold; color: #0f172a; white-space: nowrap;">
                {sym} {sname} {m_badge}
            </td>
            <td style="padding: 10px; font-weight: 600; color: #1e3a8a; white-space: nowrap;">
                {bname}
            </td>
            <td style="padding: 10px; text-align: right; font-weight: 800; color: #dc2626; white-space: nowrap; font-size: 14px;">
                +{amt_5d:,.2f} 億
            </td>
            <td style="padding: 10px; text-align: right; font-weight: 600; color: #0f172a; white-space: nowrap;">
                {amt_10d_str}
            </td>
            <td style="padding: 10px; text-align: right; font-weight: 600; color: #047857; white-space: nowrap;">
                {amt_20d_str}
            </td>
            <td style="padding: 10px; text-align: center; white-space: nowrap;">
                <span style="background-color: #f8fafc; color: #334155; border: 1px solid #cbd5e1; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 700;">
                    {m_tag}
                </span>
            </td>
            <td style="padding: 10px; font-size: 12px; font-weight: 600; color: {strat_color};">
                {strategy}
            </td>
        </tr>
        """

    whale_section_html = f"""
    <div style="margin-bottom: 28px; border: 1px solid #cbd5e1; border-radius: 10px; overflow: hidden; background: #ffffff; box-shadow: 0 4px 12px rgba(15, 23, 42, 0.05);">
        <div style="background: linear-gradient(135deg, #0c4a6e 0%, #0369a1 100%); padding: 14px 20px; color: #ffffff; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap;">
            <div>
                <div style="font-size: 16px; font-weight: 800; display: flex; align-items: center; gap: 8px;">
                    <span>🐳</span>
                    <span>【權值巨鯨籌碼追蹤矩陣】近 5 日全市場淨買超金額 TOP {len(whale_df)} 穿透</span>
                    <span style="background: rgba(255,255,255,0.2); font-size: 11px; padding: 2px 8px; border-radius: 10px; margin-left: 6px;">5d · 10d · 20d 跨週期聯動</span>
                </div>
                <div style="font-size: 12px; color: #e0f2fe; margin-top: 4px;">
                    突破單一「純度評分」對權值股之遮蔽效應，直擊外資與百億長莊在權值重兵之推進節奏與底倉厚度。
                </div>
            </div>
        </div>

        <div style="overflow-x: auto;">
            <table style="width: 100%; border-collapse: collapse; text-align: left;">
                <thead>
                    <tr style="background-color: #f8fafc; color: #475569; font-weight: 700; font-size: 12px; border-bottom: 2px solid #e2e8f0;">
                        <th style="padding: 10px 8px; text-align: center; width: 40px; white-space: nowrap;">排名</th>
                        <th style="padding: 10px; min-width: 140px; white-space: nowrap;">股票標的</th>
                        <th style="padding: 10px; min-width: 130px; white-space: nowrap;">核心推手分點</th>
                        <th style="padding: 10px; text-align: right; min-width: 110px; white-space: nowrap;">近 5 日 (點火)</th>
                        <th style="padding: 10px; text-align: right; min-width: 110px; white-space: nowrap;">近 10 日 (雙週)</th>
                        <th style="padding: 10px; text-align: right; min-width: 110px; white-space: nowrap;">近 20 日 (月底倉)</th>
                        <th style="padding: 10px; text-align: center; min-width: 130px; white-space: nowrap;">雙週推進動能</th>
                        <th style="padding: 10px; min-width: 220px; white-space: nowrap;">操盤戰略定性與指引</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
    </div>
    """
    return whale_section_html


def generate_single_table_html(top_df: pd.DataFrame) -> str:
    """生成單一週期的表格 HTML (含點火起算日、吃貨歷時、標籤與回測報酬率/集中度)"""
    if top_df.empty:
        return '<tr><td colspan="8" style="text-align:center; padding: 18px; color: #888;">此週期無符合重押門檻之標的</td></tr>'

    table_rows_html = ""
    for idx, row in top_df.reset_index(drop=True).iterrows():
        rank = idx + 1
        rank_badge_bg = "#ff4d4f" if rank <= 3 else "#1890ff" if rank <= 5 else "#6b7280"
        
        tags = []
        tag_style = 'display: inline-block; white-space: nowrap; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: bold; margin-right: 4px; margin-top: 2px;'
        
        # 點火時間與吃貨型態標籤 (結合跨週期動能比對，杜絕語意衝突)
        accum_days = int(row.get("accum_days", 1))
        momentum_tag = str(row.get("momentum_tag") or "")
        has_long_base = any(k in momentum_tag for k in ["勻速波段", "波段高檔", "熄火", "買盤已熄火", "放緩", "節奏放緩"])

        if row["buy_ratio_pct"] >= 85 and row["buy_days"] >= 4:
            tags.append(f'<span style="background-color: #fff1f0; color: #cf1322; border: 1px solid #ffa39e; {tag_style}">⭐ 川湖重押型</span>')
        elif accum_days <= 7 or row["trade_days"] <= 3:
            if has_long_base:
                # 若跨週期比對已有雙週長波底倉，不再顯示「剛點火」，改為更精確的「波段加碼」
                tags.append(f'<span style="background-color: #f6ffed; color: #389e0d; border: 1px solid #b7eb8f; {tag_style}">🔥 波段加碼</span>')
            else:
                tags.append(f'<span style="background-color: #fff7e6; color: #d46b08; border: 1px solid #ffd591; {tag_style}">🚀 剛點火</span>')
        elif row["buy_days"] >= 3 and row["buy_ratio_pct"] >= 75:
            tags.append(f'<span style="background-color: #f6ffed; color: #389e0d; border: 1px solid #b7eb8f; {tag_style}">🔥 連續吸籌</span>')
        
        if row["buy_ratio_pct"] >= 88:
            tags.append(f'<span style="background-color: #fff0f6; color: #c41d7f; border: 1px solid #ffadd2; {tag_style}">🎯 絕對鎖碼</span>')
        if row["net_amt_yi"] >= 1.0:
            tags.append(f'<span style="background-color: #f9f0ff; color: #531dab; border: 1px solid #d3adf7; {tag_style}">💰 億級重押</span>')

        # 跨週期主力動能加速度標籤 (5d vs 10d 智慧比對)
        momentum_tag = row.get("momentum_tag")
        if pd.notna(momentum_tag) and momentum_tag:
            m_text = str(momentum_tag)
            if "突發急行軍" in m_text:
                m_bg = "#f9f0ff"; m_color = "#722ed1"; m_border = "#d3adf7"
            elif "勻速波段" in m_text:
                m_bg = "#e6f7ff"; m_color = "#096dd9"; m_border = "#91d5ff"
            elif "熄火" in m_text or "停滯" in m_text:
                m_bg = "#fffbe6"; m_color = "#d46b08"; m_border = "#ffe58f"
            elif "游資" in m_text:
                m_bg = "#feffe6"; m_color = "#ad8b00"; m_border = "#fffb8f"
            else:
                m_bg = "#f5f5f5"; m_color = "#595959"; m_border = "#d9d9d9"
            tags.append(f'<span style="background-color: {m_bg}; color: {m_color}; border: 1px solid {m_border}; {tag_style}">{m_text}</span>')

        # 點火日相對高低位置標籤 (需 close_price_files 才有數值)
        position_pct = row.get("position_in_range_pct")
        if pd.notna(position_pct):
            if position_pct <= 30:
                tags.append(f'<span style="background-color: #e6fffb; color: #08979c; border: 1px solid #87e8de; {tag_style}">📉 低檔佈局</span>')
            elif position_pct >= 70:
                tags.append(f'<span style="background-color: #fff2e8; color: #d4380d; border: 1px solid #ffbb96; {tag_style}">📈 高檔追價</span>')

        tag_html = " ".join(tags) if tags else '<span style="color:#9ca3af; font-size:11px; display:inline-block; margin-top:2px;">波段佈局</span>'

        # 市場別標籤徽章
        m_type = row.get("market", "上市")
        if m_type == "上市":
            m_badge = '<span style="background-color: #e6f7ff; color: #096dd9; border: 1px solid #91d5ff; font-size: 11px; padding: 1px 5px; border-radius: 3px; font-weight: bold; margin-left: 4px;">上市</span>'
        elif m_type == "上櫃":
            m_badge = '<span style="background-color: #f6ffed; color: #389e0d; border: 1px solid #b7eb8f; font-size: 11px; padding: 1px 5px; border-radius: 3px; font-weight: bold; margin-left: 4px;">上櫃</span>'
        else:
            m_badge = f'<span style="background-color: #f9f0ff; color: #722ed1; border: 1px solid #d3adf7; font-size: 11px; padding: 1px 5px; border-radius: 3px; font-weight: bold; margin-left: 4px;">{m_type}</span>'

        table_rows_html += f"""
        <tr style="border-bottom: 1px solid #f0f0f0;">
            <td style="padding: 10px 8px; text-align: center; white-space: nowrap;">
                <span style="background-color: {rank_badge_bg}; color: #ffffff; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: bold;">{rank}</span>
            </td>
            <td style="padding: 10px; min-width: 190px;">
                <div style="font-weight: bold; font-size: 14px; color: #111827; white-space: nowrap;">{row['symbol']} {row['stock_name']} {m_badge}</div>
                <div style="margin-top: 2px; white-space: nowrap;">{tag_html}</div>
            </td>
            <td style="padding: 10px; min-width: 160px; white-space: nowrap;">
                <div style="font-weight: 600; color: #1e40af; font-size: 13px;">{row['主力分點']}</div>
                <div style="font-size: 11px; color: #4b5563; margin-top: 2px;">
                    點火: <strong style="color: #b91c1c;">{row['ignition_date']}</strong> ({row['accum_days']}天)
                </div>
                <div style="font-size: 10px; color: #9ca3af;">進出 {row['trade_days']} 天 / 買超 {row['buy_days']} 天</div>
            </td>
            <td style="padding: 10px; text-align: right; min-width: 120px; white-space: nowrap;">
                <div style="font-weight: bold; font-size: 14px; color: #dc2626;">+{row['net_amt_yi']:,.2f} 億</div>
                <div style="font-size: 11px; color: #6b7280;">買總額 {row['buy_amt_yi']:,.2f} 億</div>
            </td>
            <td style="padding: 10px; text-align: right; min-width: 110px; white-space: nowrap;">
                <div style="font-weight: bold; font-size: 13px; color: #1f2937;">{row['net_vol_sheets']:,.1f} 張</div>
                <div style="font-size: 11px; color: #059669; font-weight: 600;">純度 {row['buy_ratio_pct']:.1f}%</div>
            </td>
            <td style="padding: 10px; text-align: right; min-width: 95px; white-space: nowrap;">
                <div style="font-weight: bold; font-size: 13px; color: #374151;">${row['buy_avg_price']:,.2f}</div>
                <div style="font-size: 10px; color: #9ca3af;">主力成本均價</div>
            </td>
            <td style="padding: 10px 8px; text-align: center; min-width: 70px; white-space: nowrap;">
                <div style="display: inline-block; background-color: #eff6ff; color: #1d4ed8; padding: 3px 6px; border-radius: 5px; font-weight: bold; font-size: 12px;">
                    {row['score']} 分
                </div>
            </td>
            <td style="padding: 10px 8px; text-align: right; min-width: 130px; white-space: nowrap;">
                {_format_cost_deviation(row.get('cost_deviation_pct'))}
                <div style="font-size: 10px; color: #9ca3af; margin-top: 2px;">{_format_period_range(row.get('period_max_gain_pct'), row.get('period_max_drawdown_pct'))}</div>
                <div style="font-size: 10px; color: #9ca3af;">集中度 {_format_concentration(row.get('concentration_pct'))}</div>
            </td>
        </tr>
        """
    return table_rows_html


def _format_return_badge(return_pct) -> str:
    """將點火後報酬率格式化為紅漲綠跌徽章 (資料缺漏時顯示 N/A)"""
    if return_pct is None or pd.isna(return_pct):
        return '<span style="color:#9ca3af; font-size:12px;">N/A</span>'
    color = "#dc2626" if return_pct >= 0 else "#16a34a"
    sign = "+" if return_pct >= 0 else ""
    return f'<span style="font-weight:bold; font-size:13px; color:{color};">{sign}{return_pct:.1f}%</span>'


def _format_cost_deviation(cost_deviation_pct) -> str:
    """將成本偏離度格式化為主力損益徽章 (資料缺漏時顯示 N/A)"""
    if cost_deviation_pct is None or pd.isna(cost_deviation_pct):
        return '<span style="color:#9ca3af; font-size:12px;">N/A</span>'
    color = "#dc2626" if cost_deviation_pct >= 0 else "#16a34a"
    sign = "+" if cost_deviation_pct >= 0 else ""
    note = "已獲利" if cost_deviation_pct >= 0 else "已套牢"
    return f'<span style="font-weight:bold; font-size:13px; color:{color};">{sign}{cost_deviation_pct:.1f}% {note}</span>'


def _format_period_range(max_gain_pct, max_drawdown_pct) -> str:
    """將持有期間最大漲幅/最大回撤格式化為文字 (資料缺漏時顯示 N/A)"""
    gain_str = f"+{max_gain_pct:.1f}%" if pd.notna(max_gain_pct) else "N/A"
    drawdown_str = f"{max_drawdown_pct:.1f}%" if pd.notna(max_drawdown_pct) else "N/A"
    return f"最高 {gain_str} ／ 最深 {drawdown_str}"


def _format_concentration(concentration_pct) -> str:
    """將分點成交集中度格式化為百分比文字 (資料缺漏時顯示 N/A)"""
    if concentration_pct is None or pd.isna(concentration_pct):
        return "N/A"
    return f"{concentration_pct:.1f}%"


def generate_multi_period_html_report(
    reports_dict: Dict[str, pd.DataFrame],
    latest_date: str = "",
    report_title: str = "台股主力四週期連續重押吸籌雷達日報",
    top_display_n: int = 15,
    extra_sections_html: str = "",
    source_tag: str = "【雲端】",
    whale_matrix_html: str = "",
    files_10d: Optional[List[str]] = None,
    files_20d: Optional[List[str]] = None
) -> str:
    """生成包含 5日 (短線)、10日 (雙週波段)、20日 (月波段)、60日 (季大戶) 之全功能 HTML 郵件內容 (預設精選 TOP N，以控制郵件長度)"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 自動生成權值巨鯨籌碼追蹤矩陣 (若未顯式傳入)
    if not whale_matrix_html and "5d" in reports_dict:
        whale_matrix_html = generate_whale_matrix_html_section(
            df_5d=reports_dict.get("5d"),
            df_10d=reports_dict.get("10d"),
            df_20d=reports_dict.get("20d"),
            files_10d=files_10d,
            files_20d=files_20d,
            top_n=5
        )
    
    sections_html = ""
    period_configs = [
        ("5d", f"🚀 【短線點火雷達】近 5 日主力快速建倉 (週線 TOP {top_display_n})", "#2563eb", "適合尋找剛進場點火、連買 3 天以上之初升段飆股"),
        ("10d", f"🔥 【雙週波段追蹤】近 10 日主力持續加碼 (雙週線 TOP {top_display_n})", "#059669", "介於短線點火與月波段之間，抓取連續兩週不間斷吃貨之標的"),
        ("20d", f"⭐ 【黃金波段認養】近 20 日主力深度重押 (月線 TOP {top_display_n} ⭐川湖核心模型)", "#d97706", "籌碼沉澱最完整、主力成本均價最精準之主力飆股"),
        ("60d", f"💎 【季線超級大戶】近 60 日大波段鎖碼 (季線 TOP {top_display_n})", "#7c3aed", "億元級超級大戶數月默默吃貨、籌碼徹底鎖定之長波飆股")
    ]

    for key, title, theme_color, desc in period_configs:
        sub_df = reports_dict.get(key, pd.DataFrame())
        data_period_str = ""
        backtest_str = ""
        if not sub_df.empty and "first_date" in sub_df.columns and "last_date" in sub_df.columns:
            data_period_str = f" · 觀察區間: {sub_df['first_date'].min()} ~ {sub_df['last_date'].max()}"
        if not sub_df.empty and "return_pct" in sub_df.columns and sub_df["return_pct"].notna().any():
            valid_ret = sub_df["return_pct"].dropna()
            win_rate = (valid_ret > 0).mean() * 100
            avg_ret = valid_ret.mean()
            backtest_str = f" · 📊回測勝率 {win_rate:.0f}% / 平均報酬 {avg_ret:+.1f}%"
        
        top_list_df = sub_df.head(top_display_n)
        rows_html = generate_single_table_html(top_list_df)
        backtest_html = f'<div style="font-size: 12px; color: #0f172a; margin-top: 2px; font-weight: 600;">{backtest_str}</div>' if backtest_str else ""

        sections_html += f"""
        <div style="margin-bottom: 28px; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; background: #ffffff;">
            <div style="background-color: #f8fafc; padding: 14px 18px; border-bottom: 2px solid {theme_color}; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap;">
                <div>
                    <div style="font-size: 15px; font-weight: 800; color: #0f172a;">{title}</div>
                    <div style="font-size: 12px; color: #64748b; margin-top: 2px;">{desc}{data_period_str}</div>
                    {backtest_html}
                </div>
            </div>

            <div style="overflow-x: auto;">
                <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 13px;">
                    <thead>
                        <tr style="background-color: #f1f5f9; color: #475569; font-weight: 700; border-bottom: 1px solid #cbd5e1;">
                            <th style="padding: 8px; text-align: center; width: 40px; white-space: nowrap;">排名</th>
                            <th style="padding: 8px 10px; min-width: 190px; white-space: nowrap;">股票標的 / 吸籌特徵</th>
                            <th style="padding: 8px 10px; min-width: 160px; white-space: nowrap;">主力分點 / 點火日</th>
                            <th style="padding: 8px 10px; text-align: right; min-width: 120px; white-space: nowrap;">淨買超金額</th>
                            <th style="padding: 8px 10px; text-align: right; min-width: 110px; white-space: nowrap;">淨買張數 / 純度</th>
                            <th style="padding: 8px 10px; text-align: right; min-width: 95px; white-space: nowrap;">主力買均價</th>
                            <th style="padding: 8px; text-align: center; min-width: 70px; white-space: nowrap;">吸籌評分</th>
                            <th style="padding: 8px 10px; text-align: right; min-width: 130px; white-space: nowrap;">成本偏離度 / 波段區間 / 集中度</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{report_title}</title>
</head>
<body style="margin: 0; padding: 0; background-color: #f3f4f6; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, 'Microsoft JhengHei', sans-serif;">
    <div style="max-width: 1200px; margin: 20px auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08); border: 1px solid #e5e7eb;">
        
        <!-- Header 區塊 -->
        <div style="background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); padding: 26px 24px; color: #ffffff; border-bottom: 4px solid #3b82f6;">
            <div>
                <span style="background-color: #3b82f6; color: #ffffff; font-size: 11px; font-weight: bold; padding: 3px 8px; border-radius: 16px; letter-spacing: 0.5px;">
                    MULTI-PERIOD QUANT RADAR
                </span>
                <span style="background-color: #0284c7; color: #ffffff; font-size: 11px; font-weight: bold; padding: 3px 8px; border-radius: 16px; margin-left: 6px;">{source_tag}</span>
                <h1 style="margin: 10px 0 6px 0; font-size: 22px; font-weight: 800; letter-spacing: -0.5px; color: #ffffff;">
                    🚀 {source_tag} {report_title} ({latest_date})
                </h1>
                <p style="margin: 0; font-size: 13px; color: #94a3b8;">
                    四維度同步掃描：近 5 日短線點火 ＋ 近 10 日雙週波段 ＋ 近 20 日月波段認養 (川湖模型) ＋ 近 60 日季線大戶鎖碼
                </p>
            </div>
            
            <div style="margin-top: 14px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.1); font-size: 12px; color: #cbd5e1; display: flex; flex-wrap: wrap; gap: 16px;">
                <span>⏱ 產出時間：<strong>{now_str}</strong></span>
                <span>📎 附件：隨信附上四週期完整 Excel 複盤明細 (含主力點火起算日與吃貨歷時)</span>
            </div>
        </div>

        <!-- 主體內容 (權值巨鯨矩陣 + 4 個週期排行榜) -->
        <div style="padding: 24px 20px 10px 20px;">
            {whale_matrix_html}
            {sections_html}
        </div>

        {extra_sections_html}

        <!-- 說明與附件提示 (操盤白話文指南) -->
        <div style="padding: 16px 24px; background-color: #f8fafc; border-top: 1px solid #e2e8f0; font-size: 12px; color: #64748b; line-height: 1.6;">
            <div style="font-weight: bold; color: #334155; margin-bottom: 6px;">💡 操盤白話文快速看懂：</div>
            <ul style="margin: 0; padding-left: 18px;">
                <li><strong>主力點火日 / 吃貨歷時</strong>：系統智慧演算法偵測主力「首次大額建倉/買超爆發」的真實起算日，並計算建倉歷時天數，幫助判斷是剛進場的新主力或長線大戶。</li>
                <li><strong>買進純度（%）</strong>：主力進出的 100 張裡面，買進佔了幾張。純度超過 <strong>75%（7成5）</strong> 代表主力「只買不賣、真心吃貨」，不是當沖客！</li>
                <li><strong>主力買均價</strong>：這段期間大戶買進的「平均每股成本」。只要股價回到這個價位附近，主力通常會強力護盤防守。</li>
                <li><strong>完整明細</strong>：全市場所有符合條件的個股已整理在隨信附上的 <strong>Excel 檔案</strong>（內含 5日、10日、20日、60日 四個工作表），可直接下載打開複盤！</li>
            </ul>
        </div>

        <!-- Footer -->
        <div style="padding: 16px 24px; background-color: #0f172a; text-align: center; font-size: 12px; color: #94a3b8;">
            台股量化分點分析系統 | 自動化多週期報告引擎 · 系統自動發送請勿回覆
        </div>

    </div>
</body>
</html>
"""
    return html


def generate_multi_sheet_excel(reports_dict: Dict[str, pd.DataFrame], output_excel_path: str):
    """匯出包含 5日、20日、60日 三個工作表的 Excel (.xlsx)"""
    sheet_name_map = {
        "5d": "近5日短線點火",
        "10d": "近10日雙週波段",
        "20d": "近20日月波段重押",
        "60d": "近60日季線大戶"
    }

    export_cols = {
        "股票標的": "股票標的",
        "market": "市場別",
        "主力分點": "主力券商分點",
        "ignition_date": "主力點火起算日",
        "accum_days": "吃貨歷時(天)",
        "first_date": "資料區間起日",
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
        "buy_amt_yi": "買進總額(億元)",
        "net_amt_yi": "淨買超金額(億元)",
        "score": "主力吸籌強度評分",
        "ignition_close": "點火日收盤價",
        "latest_close": "最新收盤價",
        "cost_deviation_pct": "成本偏離度(%)",
        "period_max_gain_pct": "期間最大漲幅(%)",
        "period_max_drawdown_pct": "期間最大回撤(%)",
        "position_in_range_pct": "點火日高低位置(%)",
        "return_pct": "點火後報酬率(%)",
        "concentration_pct": "分點成交集中度(%)"
    }

    with pd.ExcelWriter(output_excel_path, engine="openpyxl") as writer:
        for key, sheet_name in sheet_name_map.items():
            df = reports_dict.get(key, pd.DataFrame())
            if df.empty:
                pd.DataFrame({"狀態": ["此週期無符合門檻之標的"]}).to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                out_df = df[[c for c in export_cols.keys() if c in df.columns]].copy()
                out_df.rename(columns=export_cols, inplace=True)
                out_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"[✓] 四週期多工作表 Excel 報表已匯出至: {output_excel_path}")


def generate_excel_report(df: pd.DataFrame, output_excel_path: str):
    """匯出單一分析清單至 Excel (.xlsx)"""
    if df.empty:
        pd.DataFrame({"狀態": ["本日無符合門檻之標的"]}).to_excel(output_excel_path, index=False)
        return

    export_cols = {
        "股票標的": "股票標的",
        "market": "市場別",
        "主力分點": "主力券商分點",
        "ignition_date": "主力點火起算日",
        "accum_days": "吃貨歷時(天)",
        "first_date": "資料區間起日",
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
        "buy_amt_yi": "買進總額(億元)",
        "net_amt_yi": "淨買超金額(億元)",
        "score": "主力吸籌強度評分",
        "ignition_close": "點火日收盤價",
        "latest_close": "最新收盤價",
        "cost_deviation_pct": "成本偏離度(%)",
        "period_max_gain_pct": "期間最大漲幅(%)",
        "period_max_drawdown_pct": "期間最大回撤(%)",
        "position_in_range_pct": "點火日高低位置(%)",
        "return_pct": "點火後報酬率(%)",
        "concentration_pct": "分點成交集中度(%)"
    }

    out_df = df[[c for c in export_cols.keys() if c in df.columns]].copy()
    out_df.rename(columns=export_cols, inplace=True)
    out_df.to_excel(output_excel_path, index=False, engine="openpyxl")
    print(f"[✓] 完整 Excel 報表已匯出至: {output_excel_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="雲端量化報告生成器")
    parser.add_argument("--data-dir", default="./output", help="Parquet 資料目錄")
    parser.add_argument("--lookback-days", type=int, default=5, help="分析天期")
    parser.add_argument("--dry-run", action="store_true", help="本地測試並產生預覽 HTML")
    args = parser.parse_args()

    all_files = sorted(glob.glob(os.path.join(args.data_dir, "*.parquet")))
    absr1_all = [
        f for f in all_files
        if "finmind" not in os.path.basename(f).lower() and "close1" not in os.path.basename(f).lower()
    ]
    close_all = [
        f for f in all_files
        if "close1" in os.path.basename(f).lower()
    ]

    if not absr1_all:
        print(f"[!] 目錄 {args.data_dir} 中無分點 Parquet 檔案。")
        sys.exit(0)

    target_files = absr1_all[-args.lookback_days:] if len(absr1_all) >= args.lookback_days else absr1_all
    target_close = close_all[-args.lookback_days:] if len(close_all) >= args.lookback_days else close_all

    print(f"[*] 鎖定近 {len(target_files)} 個交易日分點資料，開始執行重押分析...")
    res_df, summary_info = run_heavy_accumulation_analysis(target_files, close_price_files=target_close)

    html_content = generate_multi_period_html_report({"5d": res_df})
    preview_path = os.path.join(args.data_dir, "preview_report.html")
    with open(preview_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"[✓] HTML 郵件預覽檔已生成: {preview_path}")

    excel_path = os.path.join(args.data_dir, "heavy_accumulation_report.xlsx")
    generate_excel_report(res_df, excel_path)

