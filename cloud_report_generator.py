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
from find_similar_cases import get_stock_name_map, get_broker_name_map


def run_heavy_accumulation_analysis(
    parquet_files: List[str],
    min_net_amt_yi: float = 0.3,         # 淨買超金額門檻 (億元，預設 3000 萬元 = 0.3 億)
    min_buy_ratio_pct: float = 70.0,     # 買進純度佔比門檻 (預設 70%)
    min_net_vol_sheets: float = 50.0,    # 淨買超張數門檻 (預設 50 張)
    min_trade_days: int = 1,             # 最小活躍天數
    top_n: int = 20
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    透過 DuckDB 執行川湖+凱基三多重押模型分析
    回傳 (篩選結果 DataFrame, 統計數據概覽字典)
    """
    if not parquet_files:
        return pd.DataFrame(), {}

    # 過濾出標準分點彙總檔案 (api_absr1)
    absr1_files = [f.replace("\\", "/") for f in parquet_files if "finmind" not in os.path.basename(f).lower()]
    if not absr1_files:
        absr1_files = [f.replace("\\", "/") for f in parquet_files]

    stock_names = get_stock_name_map()
    broker_names = get_broker_name_map()

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
        FROM read_parquet({absr1_files})
        WHERE (buy_amt >= 100 OR sell_amt >= 100)
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
    ORDER BY net_amt_yi DESC
    """

    df = duckdb.query(sql).to_df()
    if df.empty:
        return pd.DataFrame(), {"total_records": 0, "unique_stocks": 0}

    # 計算吸籌強度評分 (Score 0~100)
    amt_score = np.clip(np.log10(np.maximum(1.0, df["net_amt_yi"] * 10000.0)) * 8.0, 0, 40.0)
    ratio_score = np.clip((df["buy_ratio_pct"] / 100.0 - 0.5) * 60.0, 0, 30.0)
    day_score = np.clip(df["buy_day_pct"] * 0.3, 0, 30.0)
    df["score"] = (amt_score + ratio_score + day_score).round(1)

    df["stock_name"] = df["symbol"].apply(lambda s: stock_names.get(s, ""))
    df["broker_name"] = df["broker_id"].apply(lambda b: broker_names.get(b, ""))

    df["股票標的"] = df.apply(lambda r: f"{r['symbol']} {r['stock_name']}".strip(), axis=1)
    df["主力分點"] = df.apply(lambda r: f"{r['broker_id']} {r['broker_name']}".strip(), axis=1)

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
        "total_heavy_amt_yi": round(df["net_amt_yi"].sum(), 2)
    }

    return df, summary


def generate_html_email_report(
    df: pd.DataFrame,
    summary: Dict[str, Any],
    report_title: str = "台股主力波段連續重押吸籌雷達日報"
) -> str:
    """生成現代 FinTech 響應式 HTML 郵件內容"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    scan_period = f"{summary.get('start_date', '')} ~ {summary.get('end_date', '')}"
    
    top_df = df.head(15) if not df.empty else pd.DataFrame()

def generate_single_table_html(top_df: pd.DataFrame) -> str:
    """生成單一週期的表格 HTML"""
    if top_df.empty:
        return '<tr><td colspan="7" style="text-align:center; padding: 18px; color: #888;">此週期無符合重押門檻之標的</td></tr>'

    table_rows_html = ""
    for idx, row in top_df.reset_index(drop=True).iterrows():
        rank = idx + 1
        rank_badge_bg = "#ff4d4f" if rank <= 3 else "#1890ff" if rank <= 5 else "#6b7280"
        
        tags = []
        tag_style = 'display: inline-block; white-space: nowrap; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: bold; margin-right: 4px; margin-top: 2px;'
        if row["buy_ratio_pct"] >= 85:
            tags.append(f'<span style="background-color: #fff1f0; color: #cf1322; border: 1px solid #ffa39e; {tag_style}">🎯 絕對鎖碼</span>')
        if row["buy_days"] >= 3:
            tags.append(f'<span style="background-color: #f6ffed; color: #389e0d; border: 1px solid #b7eb8f; {tag_style}">🔥 連續吸籌</span>')
        if row["net_amt_yi"] >= 1.0:
            tags.append(f'<span style="background-color: #f9f0ff; color: #531dab; border: 1px solid #d3adf7; {tag_style}">💰 億級重押</span>')
        
        tag_html = " ".join(tags) if tags else '<span style="color:#9ca3af; font-size:11px; display:inline-block; margin-top:2px;">標準吸籌</span>'

        table_rows_html += f"""
        <tr style="border-bottom: 1px solid #f0f0f0;">
            <td style="padding: 10px 8px; text-align: center; white-space: nowrap;">
                <span style="background-color: {rank_badge_bg}; color: #ffffff; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: bold;">{rank}</span>
            </td>
            <td style="padding: 10px; min-width: 190px;">
                <div style="font-weight: bold; font-size: 14px; color: #111827; white-space: nowrap;">{row['股票標的']}</div>
                <div style="margin-top: 2px; white-space: nowrap;">{tag_html}</div>
            </td>
            <td style="padding: 10px; min-width: 150px; white-space: nowrap;">
                <div style="font-weight: 600; color: #1e40af; font-size: 13px;">{row['主力分點']}</div>
                <div style="font-size: 11px; color: #6b7280;">進出 {row['trade_days']} 天 / 買超 {row['buy_days']} 天</div>
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
        </tr>
        """
    return table_rows_html


def generate_multi_period_html_report(
    reports_dict: Dict[str, pd.DataFrame],
    latest_date: str = "",
    report_title: str = "台股主力三週期連續重押吸籌雷達日報"
) -> str:
    """生成包含 5日 (短線)、20日 (月波段)、60日 (季大戶) 之全功能 HTML 郵件內容"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    sections_html = ""
    period_configs = [
        ("5d", "🚀 【短線點火雷達】近 5 日主力快速建倉 (週線 TOP 10)", "#2563eb", "適合尋找剛進場點火、連買 3 天以上之初升段飆股"),
        ("20d", "⭐ 【黃金波段認養】近 20 日主力深度重押 (月線 TOP 10 ⭐川湖核心模型)", "#d97706", "籌碼沉澱最完整、主力成本均價最精準之主力飆股"),
        ("60d", "💎 【季線超級大戶】近 60 日大波段鎖碼 (季線 TOP 10)", "#7c3aed", "億元級超級大戶數月默默吃貨、籌碼徹底鎖定之長波飆股")
    ]

    for key, title, theme_color, desc in period_configs:
        sub_df = reports_dict.get(key, pd.DataFrame()).head(10)
        rows_html = generate_single_table_html(sub_df)
        
        sections_html += f"""
        <div style="margin-bottom: 28px; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; background: #ffffff;">
            <div style="background-color: #f8fafc; padding: 14px 18px; border-bottom: 2px solid {theme_color}; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap;">
                <div>
                    <div style="font-size: 15px; font-weight: 800; color: #0f172a;">{title}</div>
                    <div style="font-size: 12px; color: #64748b; margin-top: 2px;">{desc}</div>
                </div>
            </div>

            <div style="overflow-x: auto;">
                <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 13px;">
                    <thead>
                        <tr style="background-color: #f1f5f9; color: #475569; font-weight: 700; border-bottom: 1px solid #cbd5e1;">
                            <th style="padding: 8px; text-align: center; width: 40px; white-space: nowrap;">排名</th>
                            <th style="padding: 8px 10px; min-width: 190px; white-space: nowrap;">股票標的 / 吸籌特徵</th>
                            <th style="padding: 8px 10px; min-width: 150px; white-space: nowrap;">主力券商分點</th>
                            <th style="padding: 8px 10px; text-align: right; min-width: 120px; white-space: nowrap;">淨買超金額</th>
                            <th style="padding: 8px 10px; text-align: right; min-width: 110px; white-space: nowrap;">淨買張數 / 純度</th>
                            <th style="padding: 8px 10px; text-align: right; min-width: 95px; white-space: nowrap;">主力買均價</th>
                            <th style="padding: 8px; text-align: center; min-width: 70px; white-space: nowrap;">吸籌評分</th>
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
    <div style="max-width: 920px; margin: 20px auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08); border: 1px solid #e5e7eb;">
        
        <!-- Header 區塊 -->
        <div style="background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); padding: 26px 24px; color: #ffffff; border-bottom: 4px solid #3b82f6;">
            <div>
                <span style="background-color: #3b82f6; color: #ffffff; font-size: 11px; font-weight: bold; padding: 3px 8px; border-radius: 16px; letter-spacing: 0.5px;">
                    MULTI-PERIOD QUANT RADAR
                </span>
                <h1 style="margin: 10px 0 6px 0; font-size: 22px; font-weight: 800; letter-spacing: -0.5px; color: #ffffff;">
                    🚀 {report_title} ({latest_date})
                </h1>
                <p style="margin: 0; font-size: 13px; color: #94a3b8;">
                    三維度同步掃描：近 5 日短線點火 ＋ 近 20 日月波段認養 (川湖模型) ＋ 近 60 日季線大戶鎖碼
                </p>
            </div>
            
            <div style="margin-top: 14px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.1); font-size: 12px; color: #cbd5e1; display: flex; flex-wrap: wrap; gap: 16px;">
                <span>⏱ 產出時間：<strong>{now_str}</strong></span>
                <span>📎 附件：隨信附上三週期完整 Excel 複盤明細 (內含 3 個工作表)</span>
            </div>
        </div>

        <!-- 主體內容 (3 個週期排行榜) -->
        <div style="padding: 24px 20px 10px 20px;">
            {sections_html}
        </div>

        <!-- 說明與附件提示 (操盤白話文指南) -->
        <div style="padding: 16px 24px; background-color: #f8fafc; border-top: 1px solid #e2e8f0; font-size: 12px; color: #64748b; line-height: 1.6;">
            <div style="font-weight: bold; color: #334155; margin-bottom: 6px;">💡 操盤白話文快速看懂：</div>
            <ul style="margin: 0; padding-left: 18px;">
                <li><strong>買進純度（%）</strong>：主力進出的 100 張裡面，買進佔了幾張。純度超過 <strong>75%（7成5）</strong> 代表主力「只買不賣、真心吃貨」，不是當沖客！</li>
                <li><strong>主力買均價</strong>：這段期間大戶買進的「平均每股成本」。只要股價回到這個價位附近，主力通常會強力護盤防守。</li>
                <li><strong>完整明細</strong>：全市場所有符合條件的個股已整理在隨信附上的 <strong>Excel 檔案</strong>（內含 5日、20日、60日 三個工作表），可直接下載打開複盤！</li>
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
        "20d": "近20日月波段重押",
        "60d": "近60日季線大戶"
    }

    export_cols = {
        "股票標的": "股票標的",
        "主力分點": "主力券商分點",
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
        "buy_amt_yi": "買進總額(億元)",
        "net_amt_yi": "淨買超金額(億元)",
        "score": "主力吸籌強度評分"
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

    print(f"[✓] 三週期多工作表 Excel 報表已匯出至: {output_excel_path}")


def generate_excel_report(df: pd.DataFrame, output_excel_path: str):
    """匯出單一分析清單至 Excel (.xlsx)"""
    if df.empty:
        pd.DataFrame({"狀態": ["本日無符合門檻之標的"]}).to_excel(output_excel_path, index=False)
        return

    export_cols = {
        "股票標的": "股票標的",
        "主力分點": "主力券商分點",
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
        "buy_amt_yi": "買進總額(億元)",
        "net_amt_yi": "淨買超金額(億元)",
        "score": "主力吸籌強度評分"
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

    files = sorted(glob.glob(os.path.join(args.data_dir, "*.parquet")))
    if not files:
        print(f"[!] 目錄 {args.data_dir} 中無 Parquet 檔案。")
        sys.exit(0)

    print(f"[*] 找到 {len(files)} 個 Parquet 檔案，開始執行重押分析...")
    res_df, summary_info = run_heavy_accumulation_analysis(files)

    html_content = generate_html_email_report(res_df, summary_info)
    preview_path = os.path.join(args.data_dir, "preview_report.html")
    with open(preview_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"[✓] HTML 郵件預覽檔已生成: {preview_path}")

    excel_path = os.path.join(args.data_dir, "heavy_accumulation_report.xlsx")
    generate_excel_report(res_df, excel_path)
