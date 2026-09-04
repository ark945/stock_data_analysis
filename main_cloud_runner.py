# -*- coding: utf-8 -*-
"""
雲端主力重押日報全流程主控模組 (Main Cloud Runner)
===================================================
執行流程：
1. 智慧增量從 Google Drive 下載近 60 日全市場分點 Parquet 檔案
2. 啟動 DuckDB 同步穿透計算四大核心週期：
   - 🚀 【近 5 日短線點火】(週線主力快速建倉)
   - 🔥 【近 10 日雙週波段追擊】(雙週線主力持續加碼)
   - ⭐ 【近 20 日月波段認養】(月線主力深度重押，川湖核心模型)
   - 💎 【近 60 日季線大戶】(季線長波段鎖碼大戶)
3. 生成現代 FinTech 響應式多週期 HTML 郵件與 4 工作表之 Excel 報表
4. 透過 SMTP 與 Telegram 將日報與 Excel 附件自動發送
"""

import os
import sys
import glob
import argparse
from datetime import datetime
import pandas as pd
import numpy as np

from cloud_gdrive_downloader import download_recent_parquet_files, download_recent_close_price_files, extract_date_from_filename
from cloud_report_generator import (
    run_heavy_accumulation_analysis,
    generate_multi_period_html_report,
    generate_multi_sheet_excel
)
from chip_intelligence_analysis import (
    detect_reversal_warning,
    detect_wash_trading,
    detect_broker_sync_group,
    build_broker_profile,
    detect_cross_stock_sync_buying,
    detect_price_volume_divergence,
    generate_intelligence_html_section,
    append_intelligence_sheets_to_excel
)
from send_email_report import send_email_report, send_telegram_notify


def main():
    parser = argparse.ArgumentParser(description="台股主力重押日報雲端四週期自動化排程")
    parser.add_argument("--lookback-days", type=int, default=60, help="回溯最大交易天數 (預設: 60 日)")
    parser.add_argument("--local-dir", default="", help="指定本機資料目錄 (若指定則略過 GDrive 下載)")
    parser.add_argument("--output-dir", default="./daily_reports", help="報表產出目錄")
    parser.add_argument("--date", default="", help="指定目標交易日 (YYYY-MM-DD，若指定則以該日為最新截斷點)")
    parser.add_argument("--no-email", action="store_true", help="僅產出檔案，不寄送 Email")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")

    print("=" * 60)
    print(f"🚀 台股主力四週期連續重押吸籌雷達日報 — 雲端自動化引擎啟動")
    print(f"[*] 執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[*] 同步運算四週期: 【近 5 日短線】+ 【近 10 日雙週波段】+ 【近 20 日月波段(川湖模型)】+ 【近 60 日季線大戶】")
    print("=" * 60)

    # 1. 取得 Parquet 資料檔案
    parquet_files = []
    close_files_all = []
    if args.local_dir and os.path.exists(args.local_dir):
        print(f"[*] 使用指定本機目錄: {args.local_dir}")
        parquet_files = sorted(glob.glob(os.path.join(args.local_dir, "*.parquet")))
        close_files_all = sorted(glob.glob(os.path.join(args.local_dir, "api_close1_*.parquet")))
    else:
        print(f"[*] 正在從 Google Drive 目標資料夾拉取近 {args.lookback_days} 日數據...")
        cache_dir = "./temp_cache_parquet"
        parquet_files = download_recent_parquet_files(
            lookback_days=args.lookback_days,
            dest_dir=cache_dir
        )
        print(f"[*] 正在從 Google Drive 拉取近 {args.lookback_days} 日每日收盤價數據...")
        close_files_all = download_recent_close_price_files(
            lookback_days=args.lookback_days,
            dest_dir="./temp_cache_close"
        )
        print(f"[*] 正在從 Google Drive 拉取近 {args.lookback_days} 日融資融券數據...")
        from cloud_gdrive_downloader import download_recent_files_by_pattern
        download_recent_files_by_pattern("margin", lookback_days=args.lookback_days, dest_dir="./output_margin")
        print(f"[*] 正在從 Google Drive 拉取近 {args.lookback_days} 日期交所期貨數據...")
        download_recent_files_by_pattern("taifex", lookback_days=args.lookback_days, dest_dir="./output_taifex")
        print(f"[*] 正在從 Google Drive 拉取近 {args.lookback_days} 日集保股權分散數據...")
        download_recent_files_by_pattern("tdcc", lookback_days=args.lookback_days, dest_dir="./output_tdcc")

    if not parquet_files:
        print("[!] 未取得任何有效 Parquet 檔案，程序終止。")
        sys.exit(1)

    # 過濾出標準分點檔案 (排除 finmind 與 close1 避免 schema mismatch)
    absr1_files = [
        f for f in parquet_files
        if "finmind" not in os.path.basename(f).lower() and "close1" not in os.path.basename(f).lower()
    ]
    if not absr1_files:
        absr1_files = parquet_files

    # 依日期由舊到新排序
    absr1_files.sort(key=lambda x: extract_date_from_filename(os.path.basename(x)))
    close_files_all.sort(key=lambda x: extract_date_from_filename(os.path.basename(x)))

    if args.date:
        print(f"[*] 指定分析基準交易日: {args.date} (自動截斷此日期之後之數據)")
        absr1_files = [f for f in absr1_files if extract_date_from_filename(os.path.basename(f)) <= args.date]
        close_files_all = [f for f in close_files_all if extract_date_from_filename(os.path.basename(f)) <= args.date]

    total_files = len(absr1_files)
    print(f"[✓] 共有 {total_files} 個交易日分點 Parquet 檔案就緒！")
    if close_files_all:
        print(f"[✓] 共有 {len(close_files_all)} 個交易日每日收盤價檔案就緒，將附加回測報酬率與分點集中度欄位。")
    else:
        print(f"[!] 未找到收盤價檔案 (api_close1_*.parquet)，將略過回測報酬率/集中度欄位。")

    # 2. 切分 4 個週期檔案清單
    files_5d = absr1_files[-5:] if total_files >= 5 else absr1_files
    files_10d = absr1_files[-10:] if total_files >= 10 else absr1_files
    files_20d = absr1_files[-20:] if total_files >= 20 else absr1_files
    files_60d = absr1_files[-60:] if total_files >= 60 else absr1_files

    total_close = len(close_files_all)
    close_5d = close_files_all[-5:] if total_close >= 5 else close_files_all
    close_10d = close_files_all[-10:] if total_close >= 10 else close_files_all
    close_20d = close_files_all[-20:] if total_close >= 20 else close_files_all
    close_60d = close_files_all[-60:] if total_close >= 60 else close_files_all

    # 3. 執行四週期 DuckDB 重押模型運算
    print("[*] 正在計算 【近 5 日】 短線點火雷達...")
    df_5d, sum_5d = run_heavy_accumulation_analysis(
        parquet_files=files_5d,
        min_net_amt_yi=0.2,          # 5 日門檻: 淨買超 >= 2,000 萬元
        min_buy_ratio_pct=70.0,
        min_net_vol_sheets=30.0,
        min_trade_days=1,
        close_price_files=close_5d
    )

    print("[*] 正在計算 【近 10 日】 雙週波段追擊...")
    df_10d, sum_10d = run_heavy_accumulation_analysis(
        parquet_files=files_10d,
        min_net_amt_yi=0.35,         # 10 日門檻: 淨買超 >= 3,500 萬元
        min_buy_ratio_pct=72.0,
        min_net_vol_sheets=50.0,
        min_trade_days=2,
        close_price_files=close_10d
    )

    print("[*] 正在計算 【近 20 日】 黃金月波段認養 (⭐川湖模型)...")
    df_20d, sum_20d = run_heavy_accumulation_analysis(
        parquet_files=files_20d,
        min_net_amt_yi=0.5,          # 20 日門檻: 淨買超 >= 5,000 萬元
        min_buy_ratio_pct=75.0,
        min_net_vol_sheets=80.0,
        min_trade_days=3,
        close_price_files=close_20d
    )

    print("[*] 正在計算 【近 60 日】 季線超級大戶長波鎖碼...")
    df_60d, sum_60d = run_heavy_accumulation_analysis(
        parquet_files=files_60d,
        min_net_amt_yi=1.0,          # 60 日門檻: 淨買超 >= 1.0 億元
        min_buy_ratio_pct=75.0,
        min_net_vol_sheets=150.0,
        min_trade_days=8,
        close_price_files=close_60d
    )

    reports_dict = {
        "5d": df_5d,
        "10d": df_10d,
        "20d": df_20d,
        "60d": df_60d
    }

    latest_date = args.date if args.date else (sum_5d.get("end_date") or sum_20d.get("end_date") or today_str)

    print(f"[✓] 四週期模型分析全數完成！")
    print(f"    - 近 5 日短線點火標的: {len(df_5d):,} 組")
    print(f"    - 近 10 日雙週波段追擊標的: {len(df_10d):,} 組")
    print(f"    - 近 20 日月波段認養標的: {len(df_20d):,} 組 (川湖模型)")
    print(f"    - 近 60 日季線大戶鎖碼標的: {len(df_60d):,} 組")

    # 3.5 執行進階籌碼情報分析 (出貨預警/量價背離/雙分點同步/分點側寫/跨股布局)
    files_recent3 = absr1_files[-3:] if total_files >= 3 else absr1_files
    close_recent3 = close_files_all[-3:] if len(close_files_all) >= 3 else close_files_all
    print("[*] 正在執行進階籌碼情報分析...")
    reversal_df = detect_reversal_warning(df_60d, files_recent3)
    wash_df = detect_wash_trading(files_5d)
    sync_df = detect_broker_sync_group(files_60d, min_co_days=5, min_net_vol_sheets=50.0, min_sync_ratio_pct=50.0)
    profile_df = build_broker_profile(files_60d, close_price_files=close_60d, top_n_brokers=50)
    cross_df = detect_cross_stock_sync_buying(df_5d, min_stock_count=3, baseline_files=files_60d)
    divergence_df = detect_price_volume_divergence(files_recent3, close_recent3)
    print(f"    - 主力翻臉出貨預警: {len(reversal_df):,} 組")
    print(f"    - 量價背離偵測: {len(divergence_df):,} 組")
    print(f"    - 集團同步進出: {len(sync_df):,} 組")
    print(f"    - 跨股同步布局: {len(cross_df):,} 組")

    # 3.2 執行尾盤放量站上 VWAP 與主力逆向歸因雷達
    tail_vwap_df = pd.DataFrame()
    tail_vwap_html = ""
    try:
        from reverse_broker_matcher import (
            scan_tail_vwap_and_attribute,
            generate_tail_vwap_html_section,
            append_tail_vwap_sheet_to_excel
        )
        target_dir = args.local_dir if (args.local_dir and os.path.exists(args.local_dir)) else (
            "./temp_cache_parquet" if os.path.exists("./temp_cache_parquet") else "./output"
        )
        print(f"[*] 正在運算最新交易日 ({latest_date}) 尾盤放量站上 VWAP 主力歸因雷達...")
        tail_vwap_df = scan_tail_vwap_and_attribute(data_dir=target_dir, target_date=latest_date)
        if not tail_vwap_df.empty:
            tail_vwap_html = generate_tail_vwap_html_section(tail_vwap_df, top_n=10)
            print(f"[✓] 成功識別 {len(tail_vwap_df)} 檔尾盤強勢標的與推手分點！")
    except Exception as e:
        print(f"[!] 尾盤放量站上 VWAP 運算異常 (跳過): {e}")

    # 4. 生成四週期 HTML 郵件內容 (每週期僅 TOP 8，控制郵件長度提高可讀性)
    report_title = f"台股主力四週期連續重押吸籌雷達日報"
    intelligence_html = generate_intelligence_html_section(reversal_df, wash_df, sync_df, profile_df, cross_df, divergence_df=divergence_df)
    combined_extra_html = tail_vwap_html + intelligence_html
    html_content = generate_multi_period_html_report(
        reports_dict=reports_dict,
        latest_date=latest_date,
        report_title=report_title,
        top_display_n=8,
        extra_sections_html=combined_extra_html
    )

    # 5. 生成包含 4 個 Sheet 的 Excel 附件 (附加 5 個進階情報工作表與 1 個尾盤歸因工作表)
    excel_filename = f"主力四週期重押雷達_{latest_date}.xlsx"
    excel_path = os.path.join(args.output_dir, excel_filename)
    generate_multi_sheet_excel(reports_dict, excel_path)
    append_intelligence_sheets_to_excel(excel_path, reversal_df, wash_df, sync_df, profile_df, cross_df, divergence_df=divergence_df)
    if not tail_vwap_df.empty:
        append_tail_vwap_sheet_to_excel(excel_path, tail_vwap_df)

    # 5.3 運算籌碼衍生指標 (買賣家數差 / 資券軋空 / 散戶接刀坑 / 大盤期權) 並加入第 4 道自愈兜底
    try:
        from chip_derivatives_engine import run_derivatives_analysis_for_date
        # 第 4 道防線：自愈兜底檢測 (若當日融資券或期貨未自 Google Drive 取得，本地秒級補抓)
        margin_file = f"./output_margin/api_margin_{latest_date}_{latest_date}.parquet"
        if not os.path.exists(margin_file):
            try:
                print(f"[*] 觸發第 4 道自愈兜底：本地即刻補抓 {latest_date} 融資券...")
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "stock_data_downloader"))
                from margin_trading_crawler import download_margin_for_date
                download_margin_for_date(latest_date, output_dir="./output_margin")
            except Exception as _e:
                print(f"[!] 自愈補抓融資券提示: {_e}")

        taifex_file = f"./output_taifex/api_taifex_{latest_date}_{latest_date}.parquet"
        if not os.path.exists(taifex_file):
            try:
                print(f"[*] 觸發第 4 道自愈兜底：本地即刻補抓 {latest_date} 期交所期權...")
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "stock_data_downloader"))
                from taifex_futures_crawler import download_taifex_futures_for_date
                download_taifex_futures_for_date(latest_date, output_dir="./output_taifex")
            except Exception as _e:
                print(f"[!] 自愈補抓期貨提示: {_e}")

        print(f"[*] 正在運算最新交易日 ({latest_date}) 籌碼衍生指標 (買賣家數差/軋空/接刀)...")
        target_broker_dir = args.local_dir if (args.local_dir and os.path.exists(args.local_dir)) else "./temp_cache_parquet"
        deriv_res = run_derivatives_analysis_for_date(
            trade_date=latest_date,
            broker_dir=target_broker_dir,
            output_dir=args.output_dir
        )
        if deriv_res:
            with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                if not deriv_res.get("concentrated", pd.DataFrame()).empty:
                    deriv_res["concentrated"].to_excel(writer, sheet_name="籌碼極度集中(家數差)", index=False)
                if not deriv_res.get("squeeze", pd.DataFrame()).empty:
                    deriv_res["squeeze"].to_excel(writer, sheet_name="極品軋空候選", index=False)
                if not deriv_res.get("trap", pd.DataFrame()).empty:
                    deriv_res["trap"].to_excel(writer, sheet_name="散戶接刀套牢坑", index=False)
                if not deriv_res.get("macro", pd.DataFrame()).empty:
                    deriv_res["macro"].to_excel(writer, sheet_name="大盤微觀期權避震", index=False)
            print(f"[✓] 成功將籌碼衍生情報 (家數差/軋空/期權) 工作表追加至 Excel: {excel_path}")
    except Exception as e:
        print(f"[!] 籌碼衍生指標運算或追加異常 (跳過): {e}")

    # 儲存本機 HTML 備份
    html_backup_path = os.path.join(args.output_dir, f"multi_period_report_{latest_date}.html")
    with open(html_backup_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"[✓] 本地 HTML 報表已儲存至: {html_backup_path}")

    # 5.5 同步至 myStock 雲端戰情室 (若配置 SUPABASE_URL 與 SUPABASE_KEY)
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    has_synced_mystock = False
    if supabase_url and supabase_key:
        print("[*] 偵測到 Supabase 設定，正在將最新籌碼戰情同步至 myStock 雲端戰情室...")
        try:
            from sync_to_mystock import prepare_chip_payloads, upsert_to_supabase, purge_date_from_supabase
            target_data_dir = args.local_dir if args.local_dir and os.path.exists(args.local_dir) else "./temp_cache_parquet"
            # 無條件刪除目標交易日之舊資料，確保全新乾淨寫入不殘留
            purge_date_from_supabase(supabase_url, supabase_key, latest_date)
            payload = prepare_chip_payloads(target_data_dir, latest_date)
            upsert_to_supabase(supabase_url, supabase_key, "daily_chip_summary", payload["daily_chip_summary"], on_conflict="trade_date")
            upsert_to_supabase(supabase_url, supabase_key, "chip_accumulation_signals", payload["chip_accumulation_signals"], on_conflict="trade_date,period_days,symbol,broker_name")
            upsert_to_supabase(supabase_url, supabase_key, "chip_exit_signals", payload["chip_exit_signals"], on_conflict="trade_date,exit_type,symbol,dump_broker_name")
            upsert_to_supabase(supabase_url, supabase_key, "broker_institution_ranks", payload["broker_institution_ranks"], on_conflict="trade_date,category,broker_name,symbol")
            upsert_to_supabase(supabase_url, supabase_key, "vwap_attribution_signals", payload["vwap_attribution_signals"], on_conflict="trade_date,symbol,broker_name")
            if payload.get("chip_derivatives_signals"):
                upsert_to_supabase(supabase_url, supabase_key, "chip_derivatives_signals", payload["chip_derivatives_signals"], on_conflict="trade_date,signal_type,symbol")
            has_synced_mystock = True
            print("[✓] 成功同步最新主力情報至 myStock 雲端戰情室！")
        except Exception as e:
            print(f"[!] myStock 同步過程發生非致命異常: {e}")

    # 6. 判定資料來源 (本機 vs 雲端)
    is_gh = os.environ.get("GITHUB_ACTIONS") == "true"
    if args.local_dir:
        source_tag = "【本機】"
        source_desc = f"本機目錄 ({args.local_dir})"
    elif is_gh:
        source_tag = "【雲端】"
        source_desc = "雲端 GitHub Actions (Google Drive)"
    else:
        source_tag = "【雲端】"
        source_desc = "本機端下載雲端 Google Drive"

    # 重新渲染含來源標記之 HTML 報表
    html_content = generate_multi_period_html_report(
        reports_dict=reports_dict,
        latest_date=latest_date,
        report_title=report_title,
        top_display_n=15,
        extra_sections_html=intelligence_html,
        source_tag=source_tag
    )

    # 發送 Email 與 Telegram 推播
    if args.no_email:
        print("[*] 依參數設定 (--no-email)，跳過郵件寄送。")
    else:
        email_subject = f"🚀 {source_tag} {report_title} ({latest_date}) | 5日短線 ＋ 10日雙週波段 ＋ 20日月波段 ＋ 60日季大戶"
        success = send_email_report(
            subject=email_subject,
            html_content=html_content,
            attachment_paths=[excel_path]
        )
        
        # Telegram 簡報推播
        top_20d_stock = sum_20d.get("top_stock", "無")
        top_20d_broker = sum_20d.get("top_broker", "")
        top_20d_amt = sum_20d.get("top_amt_yi", 0.0)

        mystock_link = "\n👉 [點此在手機開啟 myStock 戰情室](https://ark945-mystock.hf.space)" if has_synced_mystock else ""

        tg_msg = (
            f"🚀 *{source_tag} {report_title} ({latest_date})*\n"
            f"📂 *資料來源*：`{source_tag}` ({source_desc})\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔥 *近 5 日短線點火*：`{len(df_5d)} 檔`\n"
            f"🔥 *近 10 日雙週波段追擊*：`{len(df_10d)} 檔`\n"
            f"⭐ *近 20 日月波段重押 (川湖)*：`{len(df_20d)} 檔`\n"
            f"   └ 最大吸籌：`{top_20d_stock}` ({top_20d_broker} +{top_20d_amt:.2f}億)\n"
            f"💎 *近 60 日季線大戶鎖碼*：`{len(df_60d)} 檔`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📧 完整四週期 HTML 郵件與 4-Sheet Excel 已寄達信箱！"
            f"{mystock_link}"
        )
        send_telegram_notify(tg_msg)

        if success:
            print("[✓] 四週期全自動化日報流程順利完成！")
        else:
            print("[!] 郵件發送未完成，請檢查 SMTP 設定。")


if __name__ == "__main__":
    main()
