# -*- coding: utf-8 -*-
"""
SMTP 郵件派發模組 (Email Dispatcher)
=====================================
功能：
1. 支援 SSL (Port 465) 與 STARTTLS (Port 587) 安全郵件傳輸
2. 支援 HTML 內容渲染與多附檔 (Excel .xlsx 報表) 掛載
3. 支援多收件人 (逗號分隔) 批次派送
"""

import os
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from typing import List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass


def send_telegram_notify(message: str) -> bool:
    """發送 Telegram 即時推播訊息"""
    import requests
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            print("[✓] Telegram 推播訊息發送成功！")
            return True
        else:
            print(f"[!] Telegram 發送失敗: {r.text}")
    except Exception as e:
        print(f"[!] Telegram 發送異常: {e}")
    return False


def send_email_report(
    subject: str,
    html_content: str,
    recipients: Optional[List[str]] = None,
    attachment_paths: Optional[List[str]] = None
) -> bool:
    """
    發送包含 HTML 內容與附件的 Email 報告 (相容 stock_data_downloader 參數命名)
    """
    mail_host = (
        os.environ.get("SMTP_SERVER") or 
        os.environ.get("MAIL_HOST") or 
        "smtp.gmail.com"
    ).strip()
    
    port_str = (
        os.environ.get("SMTP_PORT") or 
        os.environ.get("MAIL_PORT") or 
        "587"
    ).strip()
    mail_port = int(port_str)

    mail_user = (
        os.environ.get("SMTP_USER") or 
        os.environ.get("MAIL_USERNAME") or 
        ""
    ).strip()

    mail_pass = (
        os.environ.get("SMTP_PASSWORD") or 
        os.environ.get("MAIL_PASSWORD") or 
        ""
    ).strip()

    raw_recipients = (
        os.environ.get("RECEIVER_EMAIL") or 
        os.environ.get("REPORT_RECIPIENTS") or 
        ""
    ).strip()
    if not recipients:
        if raw_recipients:
            recipients = [r.strip() for r in raw_recipients.split(",") if r.strip()]
        else:
            recipients = [mail_user] if mail_user else []

    if not mail_user or not mail_pass:
        print("[!] 尚未設定 MAIL_USERNAME 或 MAIL_PASSWORD，跳過 Email 發送。")
        return False

    if not recipients:
        print("[!] 未指定收件人 (REPORT_RECIPIENTS)，無法發送。")
        return False

    print(f"==================================================")
    print(f"[*] 正在準備發送 Email 報告...")
    print(f"[*] SMTP 伺服器: {mail_host}:{mail_port}")
    print(f"[*] 發件人: {mail_user}")
    print(f"[*] 收件人: {', '.join(recipients)}")
    print(f"[*] 郵件主旨: {subject}")
    print(f"==================================================")

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = f"台股主力雷達系統 <{mail_user}>"
    msg["To"] = ", ".join(recipients)

    # HTML 內文
    msg_body = MIMEMultipart("alternative")
    html_part = MIMEText(html_content, "html", "utf-8")
    msg_body.attach(html_part)
    msg.attach(msg_body)

    # 掛載附件
    if attachment_paths:
        for att_path in attachment_paths:
            if os.path.exists(att_path):
                file_name = os.path.basename(att_path)
                try:
                    with open(att_path, "rb") as f:
                        part = MIMEApplication(f.read(), Name=file_name)
                    part["Content-Disposition"] = f'attachment; filename="{file_name}"'
                    msg.attach(part)
                    print(f"[✓] 成功掛載附件: {file_name}")
                except Exception as e:
                    print(f"[!] 附件讀取失敗 {file_name}: {e}")

    try:
        if mail_port == 465:
            server = smtplib.SMTP_SSL(mail_host, mail_port, timeout=30)
        else:
            server = smtplib.SMTP(mail_host, mail_port, timeout=30)
            server.starttls()

        server.login(mail_user, mail_pass)
        server.sendmail(mail_user, recipients, msg.as_string())
        server.quit()
        print(f"[✓] Email 報告發送成功！已送達 {len(recipients)} 位收件人。")
        return True
    except Exception as e:
        print(f"[!] Email 發送失敗: {e}")
        return False


if __name__ == "__main__":
    test_subject = "【測試】台股主力重押日報連通性測試"
    test_html = "<h1>測試成功！</h1><p>這是一封來自 stock_data_analysis 的測試郵件。</p>"
    send_email_report(test_subject, test_html)
