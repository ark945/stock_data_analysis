# -*- coding: utf-8 -*-
"""
主力出貨/逃離雷達 (Distribution & Exit Radar)
====================================================
與「主力波段吸籌雷達」(find_similar_cases.py) 互為反向指標：
1. 先圈出「長期基期窗口」內曾經高純度重押吸籌的「個股＋主力分點」組合 (真正的大戶/主力)
2. 再比對「近期偵測窗口」內同一組合是否翻臉轉為高純度賣出 (大戶下車/棄船逃離)
3. 計算「出貨嚴重度」(近期賣超金額 / 原始吸籌金額) 與「出貨損益」(近期賣出均價 vs 原買進均價)，
   協助辨識主力是「逢高獲利了結」還是「認賠殺出」
4. 產出全繁體中文欄位之終端報表與 Excel (.xlsx) / CSV 檔案
"""

import os
import sys
import glob
import time
import argparse
from typing import Optional

import duckdb
import pandas as pd
import numpy as np

from find_similar_cases import get_stock_name_map, get_stock_market_map, get_broker_name_map, save_report_safely

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def build_exit_report_html(df: pd.DataFrame, long_days: int, recent_days: int) -> str:
    """將出貨/逃離雷達結果轉為簡潔 FinTech 風格 HTML 郵件內文"""
    rows_html = ""
    for _, r in df.iterrows():
        badge_color = "#dc2626" if "認賠" in r["出貨型態"] else "#16a34a"
        rows_html += f"""
        <tr style="border-bottom:1px solid #e5e7eb;">
            <td style="padding:8px 10px; font-weight:700;">{r['股票標的']}</td>
            <td style="padding:8px 10px;">{r['主力分點']}</td>
            <td style="padding:8px 10px;">{r['原吸籌起日']}~{r['原吸籌訖日']}<br>均價 {r['原買進均價(元)']}</td>
            <td style="padding:8px 10px;">{r['近期出貨起日']}~{r['近期出貨訖日']}<br>均價 {r['近期賣出均價(元)']}</td>
            <td style="padding:8px 10px; color:#dc2626; font-weight:700;">{r['近期淨賣超金額(億元)']:.2f}億</td>
            <td style="padding:8px 10px;">{r['出貨嚴重度(%)']:.1f}%</td>
            <td style="padding:8px 10px; color:{badge_color}; font-weight:700;">{r['出貨型態']} {r['出貨損益(%)']:.1f}%</td>
            <td style="padding:8px 10px; font-weight:700;">{r['出貨危險評分']:.1f}</td>
        </tr>
        """

    html = f"""
    <html><body style="font-family:'Microsoft JhengHei',Arial,sans-serif; background:#f8fafc; padding:20px;">
        <div style="max-width:1100px; margin:0 auto; background:#ffffff; border-radius:10px; overflow:hidden; border:1px solid #e2e8f0;">
            <div style="background:#0f172a; padding:16px 22px;">
                <div style="color:#fff; font-size:18px; font-weight:800;">🚨 全市場主力出貨/逃離雷達</div>
                <div style="color:#94a3b8; font-size:12px; margin-top:4px;">長期基期 {long_days} 日曾高純度重押 → 近期 {recent_days} 日翻臉高純度賣出，共 {len(df)} 組案例</div>
            </div>
            <div style="padding:14px 22px;">
                <table style="width:100%; border-collapse:collapse; font-size:12px; color:#1e293b;">
                    <thead>
                        <tr style="background:#f1f5f9; text-align:left;">
                            <th style="padding:8px 10px;">股票標的</th>
                            <th style="padding:8px 10px;">主力分點</th>
                            <th style="padding:8px 10px;">原吸籌區間</th>
                            <th style="padding:8px 10px;">近期出貨區間</th>
                            <th style="padding:8px 10px;">近期淨賣超</th>
                            <th style="padding:8px 10px;">出貨嚴重度</th>
                            <th style="padding:8px 10px;">出貨型態/損益</th>
                            <th style="padding:8px 10px;">危險評分</th>
                        </tr>
                    </thead>
                    <tbody>{rows_html}</tbody>
                </table>
                <div style="color:#9ca3af; font-size:11px; margin-top:12px;">完整明細請見附件 Excel 檔案。</div>
            </div>
        </div>
    </body></html>
    """
    return html


def _list_absr1_files(data_dir: str):
    """取得資料夾內所有分點日成交明細檔 (排除收盤價與 finmind 快取檔)"""
    raw_files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    return [
        f.replace("\\", "/") for f in raw_files
        if "api_absr1_" in os.path.basename(f).lower()
    ]


def scan_exit_distribution(
    data_dir: str,
    long_days: int = 60,
    recent_days: int = 5,
    min_long_net_amt_yi: float = 0.5,
    min_long_buy_ratio_pct: float = 70.0,
    min_recent_sell_amt_yi: float = 0.3,
    min_recent_sell_ratio_pct: float = 60.0,
    top_n: int = 30,
    sort_by: str = "severity",
    ignition_threshold_ratio: float = 0.20,
    exclude_etf: bool = True,
    symbol_filter: Optional[str] = None,
    broker_filter: Optional[str] = None,
    target_date: Optional[str] = None
) -> pd.DataFrame:
    """
    掃描「長期吸籌 -> 近期翻臉出貨」的個股＋主力分點組合
    sort_by: "severity" (出貨嚴重度優先，預設推薦) 或 "amt" (近期賣超金額優先)
    「原吸籌起日」採用與 find_similar_cases.py 相同的累計金額門檻演算法 (cum_net_amt 首度突破
    total_net_amt * ignition_threshold_ratio 之當日)，而非直接取資料窗口第一天，避免長期基期
    窗口邊界造成「起日全部集中在窗口起始日」的失真假象 (注意：仍受限於窗口涵蓋範圍，若主力於窗口
    開始前即已進場，本系統只能看到窗口起始日之後的資料，無法回溯窗口以外的歷史)
    """
    all_files = _list_absr1_files(data_dir)
    if not all_files:
        print(f"[!] 於目錄 {data_dir} 未找到任何 Parquet 檔案。")
        return pd.DataFrame()

    if target_date:
        import re
        all_files = [f for f in all_files if (re.search(r'\d{4}-\d{2}-\d{2}', os.path.basename(f)) and re.search(r'\d{4}-\d{2}-\d{2}', os.path.basename(f)).group(0) <= target_date)]

    long_files = all_files[-long_days:] if len(all_files) >= long_days else all_files
    recent_files = all_files[-recent_days:] if len(all_files) >= recent_days else all_files

    extra_filter = " AND symbol NOT IN ('ZZZZ', 'REG99', 'OTC99', 'Y9999')"
    if symbol_filter:
        extra_filter += f" AND symbol = '{symbol_filter.strip()}'"
    if broker_filter:
        extra_filter += f" AND broker_id = '{broker_filter.strip()}'"
    if exclude_etf:
        extra_filter += " AND NOT (symbol LIKE '00%')"

    print("=" * 60)
    print("[*] 主力出貨/逃離雷達啟動 (反向重押偵測)")
    print(f"[*] 長期基期窗口: {len(long_files)} 個交易日 ({long_files[0].split('/')[-1][10:20] if long_files else '-'} ~ {long_files[-1].split('/')[-1][10:20] if long_files else '-'}) / 近期偵測窗口: {len(recent_files)} 個交易日")
    print(f"[*] 長期門檻: 原淨買超 >= {min_long_net_amt_yi:.2f}億, 原買進佔比 >= {min_long_buy_ratio_pct:.0f}%, 起日偵測門檻 = {ignition_threshold_ratio*100:.0f}%")
    print(f"[*] 近期門檻: 近期淨賣超 >= {min_recent_sell_amt_yi:.2f}億, 近期賣出佔比 >= {min_recent_sell_ratio_pct:.0f}%")
    print("=" * 60)

    start_t = time.time()

    long_sql = f"""
        WITH raw_trades AS (
            SELECT symbol, broker_id, SUBSTRING(CAST(trade_date AS VARCHAR),1,10) AS trade_date,
                buy_vol, sell_vol, net_vol, buy_amt, sell_amt, net_amt
            FROM read_parquet({long_files})
            WHERE (buy_amt >= 200 OR sell_amt >= 200){extra_filter}
        ),
        daily_trades AS (
            SELECT symbol, broker_id, trade_date,
                SUM(buy_vol) AS buy_vol, SUM(sell_vol) AS sell_vol, SUM(net_vol) AS net_vol,
                SUM(buy_amt) AS buy_amt, SUM(sell_amt) AS sell_amt, SUM(net_amt) AS net_amt
            FROM raw_trades
            GROUP BY symbol, broker_id, trade_date
        ),
        daily_cum AS (
            SELECT *,
                SUM(net_amt) OVER (PARTITION BY symbol, broker_id ORDER BY trade_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_net_amt
            FROM daily_trades
        ),
        summary_per_pair AS (
            SELECT symbol, broker_id,
                MIN(trade_date) AS long_first_date,
                MAX(trade_date) AS long_last_date,
                SUM(net_amt) AS total_net_amt_k,
                ROUND(SUM(buy_amt)*1000.0/NULLIF(SUM(buy_vol),0),2) AS long_buy_avg_price,
                ROUND(SUM(buy_vol)*100.0/NULLIF(SUM(buy_vol)+SUM(sell_vol),0),1) AS long_buy_ratio_pct,
                ROUND(SUM(net_vol)/1000.0,1) AS long_net_sheets,
                ROUND(SUM(net_amt)/100000.0,2) AS long_net_amt_yi
            FROM daily_trades
            GROUP BY symbol, broker_id
        ),
        ignition_dates AS (
            SELECT d.symbol, d.broker_id, MIN(d.trade_date) AS ignition_date
            FROM daily_cum d
            JOIN summary_per_pair s ON d.symbol = s.symbol AND d.broker_id = s.broker_id
            WHERE d.cum_net_amt >= (s.total_net_amt_k * {ignition_threshold_ratio})
            GROUP BY d.symbol, d.broker_id
        )
        SELECT s.symbol, s.broker_id,
            COALESCE(i.ignition_date, s.long_first_date) AS long_first_date,
            s.long_last_date, s.long_buy_avg_price, s.long_buy_ratio_pct, s.long_net_sheets, s.long_net_amt_yi
        FROM summary_per_pair s
        LEFT JOIN ignition_dates i ON s.symbol = i.symbol AND s.broker_id = i.broker_id
        WHERE s.long_net_amt_yi >= {min_long_net_amt_yi}
          AND s.long_buy_ratio_pct >= {min_long_buy_ratio_pct}
    """
    long_df = duckdb.query(long_sql).to_df()

    recent_sql = f"""
        SELECT symbol, broker_id,
            MIN(SUBSTRING(CAST(trade_date AS VARCHAR),1,10)) AS recent_first_date,
            MAX(SUBSTRING(CAST(trade_date AS VARCHAR),1,10)) AS recent_last_date,
            ROUND(SUM(sell_amt)*1000.0/NULLIF(SUM(sell_vol),0),2) AS recent_sell_avg_price,
            ROUND(SUM(sell_vol)*100.0/NULLIF(SUM(buy_vol)+SUM(sell_vol),0),1) AS recent_sell_ratio_pct,
            ROUND(SUM(net_vol)/1000.0,1) AS recent_net_sheets,
            ROUND(SUM(net_amt)/100000.0,2) AS recent_net_amt_yi
        FROM read_parquet({recent_files})
        WHERE (buy_amt >= 200 OR sell_amt >= 200){extra_filter}
        GROUP BY symbol, broker_id
        HAVING SUM(net_amt)/100000.0 <= -{min_recent_sell_amt_yi}
           AND SUM(sell_vol)*100.0/NULLIF(SUM(buy_vol)+SUM(sell_vol),0) >= {min_recent_sell_ratio_pct}
    """
    recent_df = duckdb.query(recent_sql).to_df()

    elapsed = time.time() - start_t

    if long_df.empty or recent_df.empty:
        print(f"[OK] 掃描完成！耗時: {elapsed:.2f} 秒，共篩選出 0 組「大戶下車逃離」案例")
        print("=" * 60)
        return pd.DataFrame()

    res_df = long_df.merge(recent_df, on=["symbol", "broker_id"], how="inner")
    if res_df.empty:
        print(f"[OK] 掃描完成！耗時: {elapsed:.2f} 秒，共篩選出 0 組「大戶下車逃離」案例")
        print("=" * 60)
        return pd.DataFrame()

    print(f"[OK] 掃描完成！耗時: {elapsed:.2f} 秒，共篩選出 {len(res_df):,} 組「大戶下車逃離」案例")
    print("=" * 60)

    stock_names = get_stock_name_map()
    stock_markets = get_stock_market_map()
    broker_names = get_broker_name_map()

    res_df["出貨嚴重度(%)"] = (res_df["recent_net_amt_yi"].abs() / res_df["long_net_amt_yi"] * 100).clip(upper=100).round(1)
    res_df["出貨損益(%)"] = ((res_df["recent_sell_avg_price"] - res_df["long_buy_avg_price"]) / res_df["long_buy_avg_price"] * 100).round(1)
    res_df["出貨型態"] = np.where(res_df["出貨損益(%)"] >= 0, "💰 逢高獲利了結", "🩸 認賠殺出")

    amt_score = np.clip(np.log10(np.maximum(1.0, res_df["recent_net_amt_yi"].abs() * 100000.0)) * 8.0, 0, 40.0)
    severity_score = np.clip(res_df["出貨嚴重度(%)"] * 0.3, 0, 30.0)
    ratio_score = np.clip((res_df["recent_sell_ratio_pct"] / 100.0 - 0.5) * 60.0, 0, 30.0)
    res_df["出貨危險評分"] = (amt_score + severity_score + ratio_score).round(1)

    res_df["市場別"] = res_df["symbol"].apply(lambda s: stock_markets.get(s, "上市" if str(s).isdigit() and int(s) < 3000 else "上櫃"))
    res_df["股票標的"] = res_df.apply(lambda r: f"{r['symbol']}-{stock_names.get(r['symbol'], '未知')}({r['市場別']})", axis=1)
    res_df["主力分點"] = res_df["broker_id"].apply(lambda b: f"{b}-{broker_names.get(b, '未知分點')}")

    res_df.rename(columns={
        "long_first_date": "原吸籌起日",
        "long_last_date": "原吸籌訖日",
        "long_buy_avg_price": "原買進均價(元)",
        "long_buy_ratio_pct": "原買進純度(%)",
        "long_net_sheets": "原累計淨買超(張)",
        "long_net_amt_yi": "原淨買超金額(億元)",
        "recent_first_date": "近期出貨起日",
        "recent_last_date": "近期出貨訖日",
        "recent_sell_avg_price": "近期賣出均價(元)",
        "recent_sell_ratio_pct": "近期賣出佔比(%)",
        "recent_net_sheets": "近期淨賣超(張)",
        "recent_net_amt_yi": "近期淨賣超金額(億元)"
    }, inplace=True)

    ordered_cols = [
        "symbol", "broker_id", "股票標的", "市場別", "主力分點",
        "原吸籌起日", "原吸籌訖日", "原買進均價(元)", "原買進純度(%)", "原淨買超金額(億元)",
        "近期出貨起日", "近期出貨訖日", "近期賣出均價(元)", "近期賣出佔比(%)", "近期淨賣超(張)", "近期淨賣超金額(億元)",
        "出貨嚴重度(%)", "出貨損益(%)", "出貨型態", "出貨危險評分"
    ]

    if sort_by == "amt":
        res_df = res_df[ordered_cols].sort_values(by=["近期淨賣超金額(億元)", "出貨危險評分"], ascending=[True, False]).reset_index(drop=True)
    else:
        res_df = res_df[ordered_cols].sort_values(by=["出貨危險評分", "出貨嚴重度(%)"], ascending=[False, False]).reset_index(drop=True)

    return res_df.head(top_n)


def main():
    parser = argparse.ArgumentParser(description="全市場主力出貨/逃離雷達 (大戶下車反向偵測)")
    parser.add_argument("--data-dir", type=str, default=None, help="Parquet 資料夾路徑")
    parser.add_argument("--long-days", type=int, default=60, help="長期基期窗口天數 (預設 60 日，用來確認曾經是真主力)")
    parser.add_argument("--recent-days", type=int, default=5, help="近期偵測窗口天數 (預設 5 日，用來偵測翻臉出貨)")
    parser.add_argument("--min-long-amt", type=float, default=0.5, help="長期基期最小淨買超金額 (億元，預設 0.5 億)")
    parser.add_argument("--min-long-ratio", type=float, default=70.0, help="長期基期最小買進佔比 (百分比，預設 70%%)")
    parser.add_argument("--min-recent-amt", type=float, default=0.3, help="近期最小淨賣超金額 (億元，預設 0.3 億)")
    parser.add_argument("--min-recent-ratio", type=float, default=60.0, help="近期最小賣出佔比 (百分比，預設 60%%)")
    parser.add_argument("--ignition-ratio", type=float, default=0.20, help="原吸籌起日偵測門檻比例 (預設 0.20 即 20%%，方法同 find_similar_cases.py)")
    parser.add_argument("--sort", type=str, default="severity", choices=["severity", "amt"], help="排序方式: severity (出貨危險評分優先，預設) 或 amt (近期賣超金額優先)")
    parser.add_argument("--symbol", type=str, default=None, help="指定查詢特定股票 (例: 2890)")
    parser.add_argument("--broker", type=str, default=None, help="指定查詢特定券商分點 (例: 8440)")
    parser.add_argument("--date", type=str, default=None, help="指定分析基準交易日 (YYYY-MM-DD，若未指定則取最新)")
    parser.add_argument("--include-etf", action="store_true", help="包含 ETF 標的 (預設已排除 00 開頭 ETF)")
    parser.add_argument("--top", type=int, default=30, help="輸出前幾大名單 (預設 30)")
    parser.add_argument("--output", type=str, default=None, help="輸出報告路徑 (.xlsx 或 .csv)")
    parser.add_argument("--email", action="store_true", help="掃描完成後直接以 Email 寄送報告 (讀取 .env 之 SMTP 設定)")
    parser.add_argument("--email-to", type=str, default=None, help="指定收件人 (預設讀取 .env 之 RECEIVER_EMAIL)")

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

    df_top = scan_exit_distribution(
        data_dir=data_dir,
        long_days=args.long_days,
        recent_days=args.recent_days,
        min_long_net_amt_yi=args.min_long_amt,
        min_long_buy_ratio_pct=args.min_long_ratio,
        min_recent_sell_amt_yi=args.min_recent_amt,
        min_recent_sell_ratio_pct=args.min_recent_ratio,
        top_n=args.top,
        sort_by=args.sort,
        ignition_threshold_ratio=args.ignition_ratio,
        exclude_etf=not args.include_etf,
        symbol_filter=args.symbol,
        broker_filter=args.broker,
        target_date=args.date
    )

    if not df_top.empty:
        print("\n" + "=" * 115)
        print(f"🚨 全市場主力出貨/逃離雷達排行榜 TOP {len(df_top)}：")
        print("=" * 115)
        print(df_top.to_string(index=True))

        out_path = args.output
        if not out_path:
            timestamp_str = time.strftime("%Y%m%d_%H%M%S")
            out_path = os.path.join(os.path.dirname(__file__), "output", f"exit_distribution_cases_{timestamp_str}.xlsx")
        save_report_safely(df_top, out_path)

        if args.email:
            from send_email_report import send_email_report
            html_content = build_exit_report_html(df_top, args.long_days, args.recent_days)
            today_str = time.strftime("%Y-%m-%d")
            is_gh = os.environ.get("GITHUB_ACTIONS") == "true"
            source_tag = "【雲端】" if is_gh else "【本機】"
            subject = f"🚨 {source_tag} 台股主力出貨/逃離雷達日報 ({today_str}) | 共 {len(df_top)} 組大戶下車案例"
            recipients = [args.email_to] if args.email_to else None
            send_email_report(subject, html_content, recipients=recipients, attachment_paths=[out_path])
    else:
        print("\n[!] 本次掃描未發現符合門檻之「大戶下車逃離」案例，可嘗試放寬 --min-recent-amt 或 --min-recent-ratio 參數。")


if __name__ == "__main__":
    main()
