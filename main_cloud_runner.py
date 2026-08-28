# -*- coding: utf-8 -*-
"""
雲端主力重押日報全流程主控模組 (Main Cloud Runner)
===================================================
執行流程：
1. 智慧增量從 Google Drive 下載近 60 日全市場分點 Parquet 檔案
2. 啟動 DuckDB 同步穿透計算三大核心週期：
   - 🚀 【近 5 日短線點火】(週線主力快速建倉)
   - ⭐ 【近 20 日月波段認養】(月線主力深度重押，川湖核心模型)
   - 💎 【近 60 日季線大戶】(季線長波段鎖碼大戶)
3. 生成現代 FinTech 響應式多週期 HTML 郵件與 3 工作表之 Excel 報表
4. 透過 SMTP 與 Telegram 將日報與 Excel 附件自動發送
"""

import os
import sys
import glob
import argparse
from datetime import datetime

from cloud_gdrive_downloader import download_recent_parquet_files, extract_date_from_filename
from cloud_report_generator import (
    run_heavy_accumulation_analysis,
    generate_multi_period_html_report,
    generate_multi_sheet_excel
)
from send_email_report import send_email_report, send_telegram_notify


def main():
    parser = argparse.ArgumentParser(description="台股主力重押日報雲端三週期自動化排程")
    parser.add_argument("--lookback-days", type=int, default=60, help="回溯最大交易天數 (預設: 60 日)")
    parser.add_argument("--local-dir", default="", help="指定本機資料目錄 (若指定則略過 GDrive 下載)")
    parser.add_argument("--output-dir", default="./daily_reports", help="報表產出目錄")
    parser.add_argument("--no-email", action="store_true", help="僅產出檔案，不寄送 Email")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")

    print("=" * 60)
    print(f"🚀 台股主力三週期連續重押吸籌雷達日報 — 雲端自動化引擎啟動")
    print(f"[*] 執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[*] 同步運算三週期: 【近 5 日短線】+ 【近 20 日月波段(川湖模型)】+ 【近 60 日季線大戶】")
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

    # 過濾出標準分點檔案 (排除 finmind 避免 schema mismatch)
    absr1_files = [f for f in parquet_files if "finmind" not in os.path.basename(f).lower()]
    if not absr1_files:
        absr1_files = parquet_files

    # 依日期由舊到新排序
    absr1_files.sort(key=lambda x: extract_date_from_filename(os.path.basename(x)))
    total_files = len(absr1_files)
    print(f"[✓] 共有 {total_files} 個交易日分點 Parquet 檔案就緒！")

    # 2. 切分 3 個週期檔案清單
    files_5d = absr1_files[-5:] if total_files >= 5 else absr1_files
    files_20d = absr1_files[-20:] if total_files >= 20 else absr1_files
    files_60d = absr1_files[-60:] if total_files >= 60 else absr1_files

    # 3. 執行三週期 DuckDB 重押模型運算
    print("[*] 正在計算 【近 5 日】 短線點火雷達...")
    df_5d, sum_5d = run_heavy_accumulation_analysis(
        parquet_files=files_5d,
        min_net_amt_yi=0.2,          # 5 日門檻: 淨買超 >= 2,000 萬元
        min_buy_ratio_pct=70.0,
        min_net_vol_sheets=30.0,
        min_trade_days=1
    )

    print("[*] 正在計算 【近 20 日】 黃金月波段認養 (⭐川湖模型)...")
    df_20d, sum_20d = run_heavy_accumulation_analysis(
        parquet_files=files_20d,
        min_net_amt_yi=0.5,          # 20 日門檻: 淨買超 >= 5,000 萬元
        min_buy_ratio_pct=75.0,
        min_net_vol_sheets=80.0,
        min_trade_days=3
    )

    print("[*] 正在計算 【近 60 日】 季線超級大戶長波鎖碼...")
    df_60d, sum_60d = run_heavy_accumulation_analysis(
        parquet_files=files_60d,
        min_net_amt_yi=1.0,          # 60 日門檻: 淨買超 >= 1.0 億元
        min_buy_ratio_pct=75.0,
        min_net_vol_sheets=150.0,
        min_trade_days=8
    )

    reports_dict = {
        "5d": df_5d,
        "20d": df_20d,
        "60d": df_60d
    }

    latest_date = sum_5d.get("end_date") or sum_20d.get("end_date") or today_str

    print(f"[✓] 三週期模型分析全數完成！")
    print(f"    - 近 5 日短線點火標的: {len(df_5d):,} 組")
    print(f"    - 近 20 日月波段認養標的: {len(df_20d):,} 組 (川湖模型)")
    print(f"    - 近 60 日季線大戶鎖碼標的: {len(df_60d):,} 組")

    # 4. 生成三週期 HTML 郵件內容 (TOP 15 精選)
    report_title = f"台股主力三週期連續重押吸籌雷達日報"
    html_content = generate_multi_period_html_report(
        reports_dict=reports_dict,
        latest_date=latest_date,
        report_title=report_title,
        top_display_n=15
    )

    # 5. 生成包含 3 個 Sheet 的 Excel 附件
    excel_filename = f"主力三週期重押雷達_{latest_date}.xlsx"
    excel_path = os.path.join(args.output_dir, excel_filename)
    generate_multi_sheet_excel(reports_dict, excel_path)

    # 儲存本機 HTML 備份
    html_backup_path = os.path.join(args.output_dir, f"multi_period_report_{latest_date}.html")
    with open(html_backup_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"[✓] 本地 HTML 報表已儲存至: {html_backup_path}")

    # 6. 發送 Email 與 Telegram 推播
    if args.no_email:
        print("[*] 依參數設定 (--no-email)，跳過郵件寄送。")
    else:
        email_subject = f"🚀 {report_title} ({latest_date}) | 5日短線 ＋ 20日月波段 ＋ 60日季大戶"
        success = send_email_report(
            subject=email_subject,
            html_content=html_content,
            attachment_paths=[excel_path]
        )
        
        # Telegram 簡報推播
        top_20d_stock = sum_20d.get("top_stock", "無")
        top_20d_broker = sum_20d.get("top_broker", "")
        top_20d_amt = sum_20d.get("top_amt_yi", 0.0)

        tg_msg = (
            f"🚀 *{report_title} ({latest_date})*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔥 *近 5 日短線點火*：`{len(df_5d)} 檔`\n"
            f"⭐ *近 20 日月波段重押 (川湖)*：`{len(df_20d)} 檔`\n"
            f"   └ 最大吸籌：`{top_20d_stock}` ({top_20d_broker} +{top_20d_amt:.2f}億)\n"
            f"💎 *近 60 日季線大戶鎖碼*：`{len(df_60d)} 檔`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📧 完整三週期 HTML 郵件與 3-Sheet Excel 已寄達信箱！"
        )
        send_telegram_notify(tg_msg)

        if success:
            print("[✓] 三週期全自動化日報流程順利完成！")
        else:
            print("[!] 郵件發送未完成，請檢查 SMTP 設定。")


if __name__ == "__main__":
    main()
