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

    table_rows_html = ""
    if top_df.empty:
        table_rows_html = '<tr><td colspan="7" style="text-align:center; padding: 20px; color: #888;">今日無符合重押門檻之標的</td></tr>'
    else:
        for idx, row in top_df.iterrows():
            rank = idx + 1
            rank_badge_bg = "#ff4d4f" if rank <= 3 else "#1890ff" if rank <= 5 else "#6b7280"
            
            # 特徵標籤判斷
            tags = []
            if row["buy_ratio_pct"] >= 85:
                tags.append('<span style="background-color: #fff1f0; color: #cf1322; border: 1px solid #ffa39e; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: bold; margin-right: 4px;">🎯 絕對鎖碼</span>')
            if row["buy_days"] >= 3:
                tags.append('<span style="background-color: #f6ffed; color: #389e0d; border: 1px solid #b7eb8f; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: bold; margin-right: 4px;">🔥 連續吸籌</span>')
            if row["net_amt_yi"] >= 1.0:
                tags.append('<span style="background-color: #f9f0ff; color: #531dab; border: 1px solid #d3adf7; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: bold;">💰 億級重押</span>')
            
            tag_html = " ".join(tags) if tags else '<span style="color:#9ca3af; font-size:11px;">標準吸籌</span>'

            table_rows_html += f"""
            <tr style="border-bottom: 1px solid #f0f0f0; transition: background 0.2s;">
                <td style="padding: 12px 10px; text-align: center;">
                    <span style="background-color: {rank_badge_bg}; color: #ffffff; padding: 3px 8px; border-radius: 12px; font-size: 12px; font-weight: bold;">{rank}</span>
                </td>
                <td style="padding: 12px 10px;">
                    <div style="font-weight: bold; font-size: 15px; color: #111827;">{row['股票標的']}</div>
                    <div style="margin-top: 4px;">{tag_html}</div>
                </td>
                <td style="padding: 12px 10px;">
                    <div style="font-weight: 600; color: #1e40af; font-size: 14px;">{row['主力分點']}</div>
                    <div style="font-size: 12px; color: #6b7280;">進出 {row['trade_days']} 天 / 買超 {row['buy_days']} 天</div>
                </td>
                <td style="padding: 12px 10px; text-align: right;">
                    <div style="font-weight: bold; font-size: 15px; color: #dc2626;">+{row['net_amt_yi']:,.2f} 億</div>
                    <div style="font-size: 12px; color: #6b7280;">買進總額 {row['buy_amt_yi']:,.2f} 億</div>
                </td>
                <td style="padding: 12px 10px; text-align: right;">
                    <div style="font-weight: bold; font-size: 14px; color: #1f2937;">{row['net_vol_sheets']:,.1f} 張</div>
                    <div style="font-size: 12px; color: #059669; font-weight: 600;">純度 {row['buy_ratio_pct']:.1f}%</div>
                </td>
                <td style="padding: 12px 10px; text-align: right;">
                    <div style="font-weight: bold; font-size: 14px; color: #374151;">${row['buy_avg_price']:,.2f}</div>
                    <div style="font-size: 11px; color: #9ca3af;">主力成本均價</div>
                </td>
                <td style="padding: 12px 10px; text-align: center;">
                    <div style="display: inline-block; background-color: #eff6ff; color: #1d4ed8; padding: 4px 8px; border-radius: 6px; font-weight: bold; font-size: 13px;">
                        {row['score']} 分
                    </div>
                </td>
            </tr>
            """

    html = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{report_title}</title>
</head>
<body style="margin: 0; padding: 0; background-color: #f3f4f6; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, 'Microsoft JhengHei', sans-serif;">
    <div style="max-width: 860px; margin: 24px auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08); border: 1px solid #e5e7eb;">
        
        <!-- Header 區塊 -->
        <div style="background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); padding: 32px 28px; color: #ffffff; border-bottom: 4px solid #3b82f6;">
            <div style="display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap;">
                <div>
                    <span style="background-color: #3b82f6; color: #ffffff; font-size: 12px; font-weight: bold; padding: 4px 10px; border-radius: 20px; letter-spacing: 0.5px;">
                        QUANT RADAR REPORT
                    </span>
                    <h1 style="margin: 12px 0 6px 0; font-size: 24px; font-weight: 800; letter-spacing: -0.5px; color: #ffffff;">
                        🚀 {report_title}
                    </h1>
                    <p style="margin: 0; font-size: 13px; color: #94a3b8;">
                        核心模型：川湖 (2059) + 凱基-三多 (9275) 重押波段吸籌複製雷達
                    </p>
                </div>
            </div>
            
            <div style="margin-top: 20px; padding-top: 16px; border-top: 1px solid rgba(255,255,255,0.1); font-size: 13px; color: #cbd5e1; display: flex; flex-wrap: wrap; gap: 16px;">
                <span>📅 數據區間：<strong>{scan_period}</strong> (掃描 {summary.get('scan_files_count', 0)} 個日檔案)</span>
                <span>⏱ 產出時間：<strong>{now_str}</strong></span>
            </div>
        </div>

        <!-- 關鍵指標 KPI 看板 -->
        <div style="padding: 24px 28px; background-color: #f8fafc; border-bottom: 1px solid #e2e8f0;">
            <div style="font-size: 14px; font-weight: 700; color: #475569; margin-bottom: 14px; text-transform: uppercase; letter-spacing: 0.5px;">
                📊 全市場主力重押總結看板
            </div>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px;">
                <div style="background: #ffffff; padding: 16px; border-radius: 8px; border: 1px solid #e2e8f0; text-align: center;">
                    <div style="font-size: 12px; color: #64748b; font-weight: 600;">重押個股檔數</div>
                    <div style="font-size: 24px; font-weight: 800; color: #1e293b; margin-top: 6px;">{summary.get('unique_stocks', 0)} <span style="font-size: 13px; font-weight: normal; color: #64748b;">檔</span></div>
                </div>
                <div style="background: #ffffff; padding: 16px; border-radius: 8px; border: 1px solid #e2e8f0; text-align: center;">
                    <div style="font-size: 12px; color: #64748b; font-weight: 600;">主力重押總金額</div>
                    <div style="font-size: 24px; font-weight: 800; color: #dc2626; margin-top: 6px;">${summary.get('total_heavy_amt_yi', 0):,.2f} <span style="font-size: 13px; font-weight: normal; color: #64748b;">億</span></div>
                </div>
                <div style="background: #ffffff; padding: 16px; border-radius: 8px; border: 1px solid #e2e8f0; text-align: center;">
                    <div style="font-size: 12px; color: #64748b; font-weight: 600;">最大單一吸籌標的</div>
                    <div style="font-size: 18px; font-weight: 800; color: #2563eb; margin-top: 8px;">{summary.get('top_stock', '無')}</div>
                    <div style="font-size: 12px; color: #dc2626; margin-top: 2px;">+{summary.get('top_amt_yi', 0):.2f} 億 ({summary.get('top_broker', '')})</div>
                </div>
            </div>
        </div>

        <!-- TOP 15 重押飆股雷達表格 -->
        <div style="padding: 24px 28px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                <div style="font-size: 16px; font-weight: 800; color: #0f172a;">
                    🔥 核心主力重押排行榜 (TOP 15 精選)
                </div>
                <div style="font-size: 12px; color: #64748b;">
                    依淨買超金額排序
                </div>
            </div>

            <div style="overflow-x: auto;">
                <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 13px;">
                    <thead>
                        <tr style="background-color: #f1f5f9; color: #475569; font-weight: 700; border-bottom: 2px solid #cbd5e1;">
                            <th style="padding: 10px; text-align: center; width: 45px;">排名</th>
                            <th style="padding: 10px;">股票標的 / 特徵</th>
                            <th style="padding: 10px;">主力券商分點</th>
                            <th style="padding: 10px; text-align: right;">淨買超金額</th>
                            <th style="padding: 10px; text-align: right;">淨買張數 / 純度</th>
                            <th style="padding: 10px; text-align: right;">主力買均價</th>
                            <th style="padding: 10px; text-align: center;">吸籌評分</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows_html}
                    </tbody>
                </table>
            </div>
        </div>

        <!-- 說明與附件提示 -->
        <div style="padding: 18px 28px; background-color: #f8fafc; border-top: 1px solid #e2e8f0; font-size: 12px; color: #64748b; line-height: 1.6;">
            <div style="font-weight: bold; color: #334155; margin-bottom: 4px;">💡 模型解讀指引：</div>
            <ul style="margin: 0; padding-left: 20px;">
                <li><strong>買進純度佔比</strong>：主力總買進股數佔該分點該股總進出量（買+賣）之比例，$\ge 75\%$ 代表純多單鎖碼。</li>
                <li><strong>主力買均價</strong>：波段累計買進之加權平均成本，若現價接近成本區且主力未出貨，具備極高防守與拉抬動能。</li>
                <li><strong>完整資料</strong>：全市場所有符合篩選之完整明細已自動匯出為 Excel 檔案隨信附上，歡迎下載複盤。</li>
            </ul>
        </div>

        <!-- Footer -->
        <div style="padding: 18px 28px; background-color: #0f172a; text-align: center; font-size: 12px; color: #94a3b8;">
            台股量化分點分析系統 | 自動化報告引擎 · 系統自動發送請勿回覆
        </div>

    </div>
</body>
</html>
"""
    return html


def generate_excel_report(df: pd.DataFrame, output_excel_path: str):
    """匯出完整分析清單至 Excel (.xlsx)"""
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
