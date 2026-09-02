# -*- coding: utf-8 -*-
"""
進階籌碼情報分析模組 (Chip Intelligence Analysis)
====================================================
補強主力重押雷達之外的四種進階分點情報：
1. 主力翻臉出貨預警：長期重押分點最近轉為淨賣出
2. 隔日沖/當沖雜訊偵測：同一分點同日大量對敲買賣
3. 集團同步進出偵測：不同分點同日同股同步進出之機率異常組合
4. 分點擅長股性側寫：分點過去操作股票的價格區間與集中標的
5. 跨股同步布局偵測：同一分點短期間同時點火多檔個股
"""

from typing import List, Dict, Any

import duckdb
import pandas as pd
import numpy as np

from find_similar_cases import get_stock_name_map, get_broker_name_map


def _norm(files: List[str]) -> List[str]:
    return [f.replace("\\", "/") for f in files]


def detect_reversal_warning(
    long_term_df: pd.DataFrame,
    recent_files: List[str],
    min_long_term_amt_yi: float = 0.5
) -> pd.DataFrame:
    """
    主力翻臉出貨預警：掃描長期重押名單 (20日/60日) 中，最近幾日由買轉賣的分點+股票組合
    (長期累計淨買超為正、但近期窗口淨買超轉為負值)
    """
    if long_term_df.empty or not recent_files:
        return pd.DataFrame()

    sql = f"""
        SELECT symbol, broker_id,
            SUM(net_vol) / 1000.0 AS recent_net_vol_sheets,
            SUM(net_amt) / 100000.0 AS recent_net_amt_yi,
            SUM(sell_vol) / 1000.0 AS recent_sell_vol_sheets
        FROM read_parquet({_norm(recent_files)})
        GROUP BY symbol, broker_id
    """
    recent_df = duckdb.query(sql).to_df()

    base = long_term_df[long_term_df["net_amt_yi"] >= min_long_term_amt_yi]
    merged = base.merge(recent_df, on=["symbol", "broker_id"], how="inner")
    warning_df = merged[merged["recent_net_amt_yi"] < 0].copy()
    if warning_df.empty:
        return warning_df

    warning_df["reversal_severity_pct"] = (
        warning_df["recent_net_amt_yi"].abs() / warning_df["net_amt_yi"] * 100
    ).round(1)
    warning_df.sort_values("recent_net_amt_yi", inplace=True)

    keep_cols = ["股票標的", "主力分點", "net_amt_yi", "buy_ratio_pct", "score",
                 "recent_net_amt_yi", "recent_sell_vol_sheets", "reversal_severity_pct"]
    keep_cols = [c for c in keep_cols if c in warning_df.columns]
    return warning_df[keep_cols].reset_index(drop=True)


def detect_wash_trading(
    files: List[str],
    min_vol_sheets: float = 100.0,
    min_overlap_ratio: float = 0.7
) -> pd.DataFrame:
    """
    隔日沖/當沖雜訊偵測：找出同一分點、同一股票、同一天同時出現大量買進與賣出 (對敲/當沖型態)
    overlap_ratio = min(買進,賣出) / max(買進,賣出)，越接近 1 代表越像同進同出的雜訊單，非方向性重押
    """
    if not files:
        return pd.DataFrame()

    stock_names = get_stock_name_map()
    broker_names = get_broker_name_map()

    sql = f"""
        SELECT symbol, broker_id, SUBSTRING(CAST(trade_date AS VARCHAR), 1, 10) AS trade_date,
            SUM(buy_vol) / 1000.0 AS buy_vol_sheets,
            SUM(sell_vol) / 1000.0 AS sell_vol_sheets
        FROM read_parquet({_norm(files)})
        WHERE NOT (symbol LIKE '00%')
        GROUP BY symbol, broker_id, SUBSTRING(CAST(trade_date AS VARCHAR), 1, 10)
        HAVING SUM(buy_vol) / 1000.0 >= {min_vol_sheets} AND SUM(sell_vol) / 1000.0 >= {min_vol_sheets}
    """
    df = duckdb.query(sql).to_df()
    if df.empty:
        return df

    df["overlap_ratio"] = (
        df[["buy_vol_sheets", "sell_vol_sheets"]].min(axis=1) /
        df[["buy_vol_sheets", "sell_vol_sheets"]].max(axis=1)
    ).round(3)
    df = df[df["overlap_ratio"] >= min_overlap_ratio].copy()
    if df.empty:
        return df

    df["股票標的"] = df["symbol"].apply(lambda s: f"{s} {stock_names.get(s, '')}".strip())
    df["主力分點"] = df["broker_id"].apply(lambda b: f"{b} {broker_names.get(b, '')}".strip())
    df.sort_values(["overlap_ratio", "buy_vol_sheets"], ascending=[False, False], inplace=True)
    return df[["trade_date", "股票標的", "主力分點", "buy_vol_sheets", "sell_vol_sheets", "overlap_ratio"]].reset_index(drop=True)


def detect_broker_sync_group(
    files: List[str],
    min_co_days: int = 3,
    min_net_vol_sheets: float = 50.0,
    min_sync_ratio_pct: float = 50.0,
    top_n: int = 50
) -> pd.DataFrame:
    """
    集團/同步進出偵測：找出不同分點代碼，經常在同一天、同一檔股票同步大買的組合
    以 sync_ratio (同步天數 / 兩者中較不活躍者的總顯著買超天數) 篩選，
    避免外資大型法人分點 (如美商高盛、摩根士丹利) 因廣泛佈局各大權值股而被誤判為集團
    (co_days 越高、sync_ratio 越高，代表兩個分點步調越一致，可能為同一金主或作手集團之分倉操作)
    僅納入單日淨買超達 min_net_vol_sheets 張以上的顯著買盤，避免熱門股大量分點互相配對造成組合數爆炸
    """
    if not files:
        return pd.DataFrame()

    broker_names = get_broker_name_map()

    sql = f"""
        WITH daily AS (
            SELECT symbol, broker_id, SUBSTRING(CAST(trade_date AS VARCHAR), 1, 10) AS trade_date,
                SUM(net_vol) / 1000.0 AS net_vol_sheets
            FROM read_parquet({_norm(files)})
            WHERE NOT (symbol LIKE '00%')
            GROUP BY symbol, broker_id, SUBSTRING(CAST(trade_date AS VARCHAR), 1, 10)
        ),
        buy_side AS (
            SELECT * FROM daily WHERE net_vol_sheets >= {min_net_vol_sheets}
        ),
        broker_activity AS (
            SELECT broker_id, COUNT(*) AS active_events
            FROM buy_side GROUP BY broker_id
        ),
        pairs AS (
            SELECT a.broker_id AS broker_a, b.broker_id AS broker_b,
                COUNT(*) AS co_days,
                COUNT(DISTINCT a.symbol) AS co_stocks
            FROM buy_side a
            JOIN buy_side b ON a.symbol = b.symbol AND a.trade_date = b.trade_date AND a.broker_id < b.broker_id
            GROUP BY a.broker_id, b.broker_id
            HAVING COUNT(*) >= {min_co_days}
        )
        SELECT p.broker_a, p.broker_b, p.co_days, p.co_stocks,
            ROUND(p.co_days * 100.0 / LEAST(aa.active_events, bb.active_events), 1) AS sync_ratio_pct
        FROM pairs p
        JOIN broker_activity aa ON p.broker_a = aa.broker_id
        JOIN broker_activity bb ON p.broker_b = bb.broker_id
        WHERE p.co_days * 100.0 / LEAST(aa.active_events, bb.active_events) >= {min_sync_ratio_pct}
        ORDER BY sync_ratio_pct DESC, co_days DESC
        LIMIT {top_n}
    """
    df = duckdb.query(sql).to_df()
    if df.empty:
        return df

    df["分點A"] = df["broker_a"].apply(lambda b: f"{b} {broker_names.get(b, '')}".strip())
    df["分點B"] = df["broker_b"].apply(lambda b: f"{b} {broker_names.get(b, '')}".strip())
    return df[["分點A", "分點B", "co_days", "co_stocks", "sync_ratio_pct"]].rename(
        columns={"co_days": "同步買超天數", "co_stocks": "同步標的檔數", "sync_ratio_pct": "同步比例(%)"}
    ).reset_index(drop=True)


def build_broker_profile(
    files: List[str],
    close_price_files: List[str] = None,
    min_buy_amt_yi: float = 0.1,
    top_n_brokers: int = 50
) -> pd.DataFrame:
    """
    分點擅長股性側寫：統計每個分點過去操作過的股票數、平均操作價位與偏好標的 (依買進金額排序前3檔)
    """
    if not files:
        return pd.DataFrame()

    stock_names = get_stock_name_map()
    broker_names = get_broker_name_map()

    sql = f"""
        SELECT symbol, broker_id,
            SUM(buy_vol) AS buy_vol,
            SUM(buy_amt) AS buy_amt
        FROM read_parquet({_norm(files)})
        WHERE NOT (symbol LIKE '00%')
        GROUP BY symbol, broker_id
        HAVING SUM(buy_amt) / 100000.0 >= {min_buy_amt_yi}
    """
    df = duckdb.query(sql).to_df()
    if df.empty:
        return df

    df["avg_price"] = (df["buy_amt"] * 1000.0 / df["buy_vol"]).round(2)
    df["buy_amt_yi"] = (df["buy_amt"] / 100000.0).round(2)

    profiles = []
    for broker_id, g in df.groupby("broker_id"):
        g_sorted = g.sort_values("buy_amt_yi", ascending=False)
        top_stocks = g_sorted.head(3)
        top_stock_str = "、".join(
            f"{s}{stock_names.get(s, '')}" for s in top_stocks["symbol"]
        )
        weighted_avg_price = np.average(g["avg_price"], weights=g["buy_amt_yi"]) if g["buy_amt_yi"].sum() > 0 else np.nan
        profiles.append({
            "broker_id": broker_id,
            "主力分點": f"{broker_id} {broker_names.get(broker_id, '')}".strip(),
            "操作標的檔數": g["symbol"].nunique(),
            "加權平均操作價位": round(weighted_avg_price, 1) if pd.notna(weighted_avg_price) else None,
            "總買進金額(億元)": round(g["buy_amt_yi"].sum(), 2),
            "偏好標的TOP3": top_stock_str
        })

    profile_df = pd.DataFrame(profiles)
    if profile_df.empty:
        return profile_df
    profile_df.sort_values("總買進金額(億元)", ascending=False, inplace=True)
    return profile_df.head(top_n_brokers).drop(columns=["broker_id"]).reset_index(drop=True)


def detect_cross_stock_sync_buying(
    short_term_df: pd.DataFrame,
    min_stock_count: int = 3,
    baseline_files: List[str] = None,
    max_baseline_stock_count: int = 300
) -> pd.DataFrame:
    """
    跨股同步布局偵測：找出同一分點在同一短期窗口內，同時對 N 檔以上不同個股發動點火買超
    (直接沿用短週期 (5日/10日) 重押結果 DataFrame，無需重新查詢)
    若提供 baseline_files (如近60日全量分點檔案)，會排除平常就廣泛佈局各股的「通才型」大型外資分點
    (如高盛、摩根士丹利等)，只保留平常操作標的較集中、這次卻突然同步進場多檔的「異常」分點
    """
    if short_term_df.empty or "broker_id" not in short_term_df.columns:
        return pd.DataFrame()

    grouped = short_term_df.groupby(["broker_id", "主力分點"])["symbol"].agg(
        股票數=lambda s: s.nunique(),
        標的清單=lambda s: "、".join(sorted(set(s)))
    ).reset_index()
    grouped = grouped[grouped["股票數"] >= min_stock_count]
    if grouped.empty:
        return grouped

    if baseline_files:
        baseline_sql = f"""
            WITH agg AS (
                SELECT symbol, broker_id, SUM(buy_amt) / 100000.0 AS buy_amt_yi
                FROM read_parquet({_norm(baseline_files)})
                WHERE NOT (symbol LIKE '00%')
                GROUP BY symbol, broker_id
                HAVING SUM(buy_amt) / 100000.0 >= 0.05
            )
            SELECT broker_id, COUNT(DISTINCT symbol) AS baseline_stock_count
            FROM agg GROUP BY broker_id
        """
        baseline_df = duckdb.query(baseline_sql).to_df()
        grouped = grouped.merge(baseline_df, on="broker_id", how="left")
        grouped = grouped[grouped["baseline_stock_count"].fillna(0) <= max_baseline_stock_count]
        if grouped.empty:
            return grouped
        grouped.drop(columns=["baseline_stock_count"], inplace=True)

    grouped.sort_values("股票數", ascending=False, inplace=True)
    return grouped.drop(columns=["broker_id"]).reset_index(drop=True)


def generate_intelligence_html_section(
    reversal_df: pd.DataFrame,
    wash_df: pd.DataFrame,
    sync_df: pd.DataFrame,
    profile_df: pd.DataFrame,
    cross_df: pd.DataFrame,
    top_n: int = 3
) -> str:
    """生成「進階籌碼情報」精簡摘要 HTML 區塊 (單一卡片、每類僅前3筆，完整清單請見附件 Excel)"""

    def _sub_section(title: str, color: str, items: List[str]) -> str:
        if not items:
            return ""
        rows = "".join(
            f'<div style="font-size:12px; color:#374151; padding: 3px 0;">・{it}</div>' for it in items
        )
        return f"""
            <div style="margin-top: 10px;">
                <div style="font-size: 12.5px; font-weight: 700; color: {color};">{title}</div>
                {rows}
            </div>
        """

    reversal_items = []
    if not reversal_df.empty:
        for _, r in reversal_df.head(top_n).iterrows():
            reversal_items.append(
                f"<strong>{r['股票標的']}</strong> ({r['主力分點']}) +{r['net_amt_yi']:.1f}億→轉賣{r['recent_net_amt_yi']:.1f}億"
            )

    sync_items = []
    if not sync_df.empty:
        for _, r in sync_df.head(top_n).iterrows():
            sync_items.append(
                f"<strong>{r['分點A']}</strong> ↔ <strong>{r['分點B']}</strong>：同步 {r['同步買超天數']} 天/{r['同步標的檔數']} 檔 ({r['同步比例(%)']:.0f}%)"
            )

    cross_items = []
    if not cross_df.empty:
        for _, r in cross_df.head(top_n).iterrows():
            cross_items.append(f"<strong>{r['主力分點']}</strong>：同時點火 {r['股票數']} 檔 ({r['標的清單'][:40]}{'...' if len(r['標的清單']) > 40 else ''})")

    sections = "".join([
        _sub_section("⚠️ 主力翻臉出貨預警", "#dc2626", reversal_items),
        _sub_section("🔗 集團同步進出", "#7c3aed", sync_items),
        _sub_section("🎯 跨股同步布局", "#0891b2", cross_items),
    ])

    if not sections:
        sections = '<div style="font-size:12px; color:#9ca3af; padding: 6px 0;">本期無符合條件之進階情報項目</div>'

    wash_note = f"另掃出 {len(wash_df):,} 筆同日對敲雜訊、{len(profile_df):,} 筆分點側寫，完整清單請見附件 Excel。" if not wash_df.empty else "完整清單請見附件 Excel。"

    html = f"""
        <div style="padding: 4px 20px 14px 20px;">
            <div style="border: 1px solid #e2e8f0; border-radius: 8px; background: #ffffff; padding: 12px 16px;">
                <div style="font-size: 14px; font-weight: 800; color: #0f172a;">🕵️ 進階籌碼情報摘要 (各項僅列前 {top_n} 筆)</div>
                {sections}
                <div style="font-size: 11px; color: #9ca3af; margin-top: 10px;">{wash_note}</div>
            </div>
        </div>
    """
    return html


def append_intelligence_sheets_to_excel(
    excel_path: str,
    reversal_df: pd.DataFrame,
    wash_df: pd.DataFrame,
    sync_df: pd.DataFrame,
    profile_df: pd.DataFrame,
    cross_df: pd.DataFrame
) -> None:
    """將五項進階籌碼情報以額外工作表附加到既有 Excel 報表 (不覆蓋原有工作表)"""
    sheets = {
        "出貨預警": reversal_df,
        "隔日沖雜訊": wash_df,
        "集團同步進出": sync_df,
        "分點側寫": profile_df,
        "跨股同步布局": cross_df
    }

    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        for sheet_name, df in sheets.items():
            out_df = df if not df.empty else pd.DataFrame({"狀態": ["本期無符合條件之標的"]})
            out_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"[✓] 進階籌碼情報 5 個工作表已附加至: {excel_path}")
