# -*- coding: utf-8 -*-
"""
雲端主力重押日報全流程主控模組 (Main Cloud Runner)
===================================================
執行流程：
1. 智慧增量從 Google Drive 下載近 N 日全市場分點 Parquet 檔案
2. 啟動 DuckDB 穿透計算川湖 (2059) + 凱基-三多 (9275) 重押模型
3. 生成現代響應式 HTML 郵件內容與完整 Excel 報表
4. 透過 SMTP 將日報與 Excel 附件自動發送至指定 Email 信箱
"""

import os
import sys
import glob
import argparse
from datetime import datetime

from cloud_gdrive_downloader import download_recent_parquet_files
from cloud_report_generator import (
    run_heavy_accumulation_analysis,
    generate_html_email_report,
    generate_excel_report
)
from send_email_report import send_email_report


def main():
    parser = argparse.ArgumentParser(description="台股主力重押日報雲端自動化排程")
    parser.add_argument("--lookback-days", type=int, default=5, help="回溯分析交易天數 (預設: 5 日)")
    parser.add_argument("--min-amt", type=float, default=0.3, help="最小淨買超金額門檻 (億元，預設: 0.3 億)")
    parser.add_argument("--min-ratio", type=float, default=70.0, help="最小買進純度佔比 (預設: 70%)")
    parser.add_argument("--local-dir", default="", help="指定本機資料目錄 (若指定則略過 GDrive 下載)")
    parser.add_argument("--output-dir", default="./daily_reports", help="報表產出目錄")
    parser.add_argument("--no-email", action="store_true", help="僅產出檔案，不寄送 Email")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")

    print("=" * 60)
    print(f"🚀 台股主力波段連續重押吸籌雷達日報 — 雲端自動化引擎啟動")
    print(f"[*] 執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[*] 回溯天期: 近 {args.lookback_days} 個交易日")
    print("=" * 60)

    # 1. 取得 Parquet 資料檔案
    parquet_files = []
    if args.local_dir and os.path.exists(args.local_dir):
        print(f"[*] 使用指定本機目錄: {args.local_dir}")
        parquet_files = sorted(glob.glob(os.path.join(args.local_dir, "*.parquet")))
    else:
        print(f"[*] 正在從 Google Drive 目標資料夾拉取近 {args.lookback_days} 日數據...")
        cache_dir = "./temp_cache_parquet"
        parquet_files = download_recent_parquet_files(
            lookback_days=args.lookback_days,
            dest_dir=cache_dir
        )

    if not parquet_files:
        print("[!] 未取得任何有效 Parquet 檔案，程序終止。")
        sys.exit(1)

    print(f"[✓] 共有 {len(parquet_files)} 個分點 Parquet 檔案就緒，開始進行量化模型分析...")

    # 2. 執行川湖 + 凱基三多重押模型計算
    df, summary = run_heavy_accumulation_analysis(
        parquet_files=parquet_files,
        min_net_amt_yi=args.min_amt,
        min_buy_ratio_pct=args.min_ratio,
        min_net_vol_sheets=50.0,
        min_trade_days=1
    )

    print(f"[✓] 模型分析完成！共篩選出 {len(df):,} 筆主力重押標的 (涵蓋 {summary.get('unique_stocks', 0)} 檔個股)。")

    # 3. 產出 HTML 郵件內容與 Excel 附件
    report_title = f"台股主力波段連續重押吸籌雷達日報 ({summary.get('end_date', today_str)})"
    html_content = generate_html_email_report(df, summary, report_title=report_title)

    excel_filename = f"主力重押雷達_{summary.get('end_date', today_str)}_近{args.lookback_days}日.xlsx"
    excel_path = os.path.join(args.output_dir, excel_filename)
    generate_excel_report(df, excel_path)

    # 儲存一份本機 HTML 檔案備份
    html_backup_path = os.path.join(args.output_dir, f"report_{summary.get('end_date', today_str)}.html")
    with open(html_backup_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"[✓] 本地 HTML 報表已儲存至: {html_backup_path}")

    # 4. 發送 Email 與 Telegram 推播
    if args.no_email:
        print("[*] 依參數設定 (--no-email)，跳過郵件寄送。")
    else:
        email_subject = f"🚀 {report_title} | 篩選 {summary.get('unique_stocks', 0)} 檔重押主力標的"
        success = send_email_report(
            subject=email_subject,
            html_content=html_content,
            attachment_paths=[excel_path]
        )
        
        # 同步發送 Telegram 簡報推播 (若有配置)
        from send_email_report import send_telegram_notify
        top_stock = summary.get("top_stock", "無")
        top_amt = summary.get("top_amt_yi", 0.0)
        top_broker = summary.get("top_broker", "")
        
        tg_msg = (
            f"🚀 *{report_title}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 *重押個股數*：`{summary.get('unique_stocks', 0)} 檔`\n"
            f"💰 *主力總重押*：`${summary.get('total_heavy_amt_yi', 0):,.2f} 億`\n"
            f"👑 *最大吸籌標的*：`{top_stock}`\n"
            f"   └ 主力分點：`{top_broker}` (+{top_amt:.2f} 億)\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📧 完整 HTML 郵件日報與 Excel 明細已寄送至信箱！"
        )
        send_telegram_notify(tg_msg)

        if success:
            print("[✓] 全自動化日報流程順利完成！")
        else:
            print("[!] 郵件發送未完成，請檢查 SMTP 設定。")


if __name__ == "__main__":
    main()
