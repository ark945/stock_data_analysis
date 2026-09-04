# -*- coding: utf-8 -*-
"""
Google Drive 雲端分點資料智慧下載模組 (Cloud GDrive Downloader)
================================================================
功能：
1. 連線至 Google Drive 目標資料夾，列出所有全市場分點 Parquet 檔案 (api_absr1_*.parquet)。
2. 自動按日期由新到舊排序，智慧增量下載分析所需的近 N 個交易日 Parquet 檔案。
3. 支援 Google Cloud Service Account 認證 (JSON 檔案或環境變數 GDRIVE_SERVICE_ACCOUNT_KEY)。
"""

import os
import sys
import io
import json
import re
from typing import List, Optional, Dict, Any

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def get_gdrive_service(service_account_key_raw: Optional[str] = None):
    """取得 Google Drive API Service 實例"""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        print("[!] 尚未安裝 Google API 套件，請執行: pip install google-api-python-client google-auth")
        return None

    raw_key = service_account_key_raw or os.environ.get("GDRIVE_SERVICE_ACCOUNT_KEY")
    creds = None

    if raw_key:
        raw_key = raw_key.strip()
        if os.path.exists(raw_key):
            creds = service_account.Credentials.from_service_account_file(raw_key, scopes=SCOPES)
        else:
            try:
                key_dict = json.loads(raw_key)
                creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)
            except Exception as e:
                print(f"[!] 解析 GDRIVE_SERVICE_ACCOUNT_KEY 失敗: {e}")
                return None
    else:
        for fallback_f in ["credentials.json", "service_account.json", "gdrive_key.json"]:
            local_p = os.path.join(os.path.dirname(__file__), fallback_f)
            if os.path.exists(local_p):
                try:
                    creds = service_account.Credentials.from_service_account_file(local_p, scopes=SCOPES)
                    break
                except Exception:
                    pass

    if not creds:
        return None

    try:
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"[!] 建立 Google Drive 服務實例失敗: {e}")
        return None


def extract_date_from_filename(filename: str) -> str:
    """從檔名提取 YYYY-MM-DD 或 YYYYMMDD 日期字串"""
    # 支援格式: api_absr1_2026-08-26.parquet 或 api_absr1_2026-08-26_2026-08-26.parquet
    m = re.findall(r"\d{4}[-_]\d{2}[-_]\d{2}", filename)
    if m:
        return m[-1].replace("_", "-")
    m2 = re.findall(r"\d{8}", filename)
    if m2:
        d = m2[-1]
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return filename


def list_gdrive_parquet_files(service, folder_id: str) -> List[Dict[str, Any]]:
    """列出 Google Drive 資料夾內所有分點 Parquet 檔案並按日期降序排序"""
    query = (
        f"'{folder_id}' in parents and "
        f"name contains '.parquet' and "
        f"trashed = false"
    )
    all_files = []
    page_token = None

    while True:
        results = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, size, modifiedTime)",
            pageSize=100,
            pageToken=page_token
        ).execute()

        files = results.get("files", [])
        for f in files:
            name = f.get("name", "")
            if name.endswith(".parquet"):
                f_date = extract_date_from_filename(name)
                all_files.append({
                    "id": f.get("id"),
                    "name": name,
                    "size": int(f.get("size", 0)),
                    "date": f_date,
                    "modified": f.get("modifiedTime")
                })

        page_token = results.get("nextPageToken")
        if not page_token:
            break

    # 按日期降序排列（最新日期在最前面）
    all_files.sort(key=lambda x: x["date"], reverse=True)
    return all_files


def download_recent_parquet_files(
    folder_id: Optional[str] = None,
    lookback_days: int = 5,
    dest_dir: str = "./cloud_data"
) -> List[str]:
    """
    下載 Google Drive 雲端中最近 N 個交易日的 Parquet 檔案
    回傳本地下載後的檔案路徑列表
    """
    from googleapiclient.http import MediaIoBaseDownload

    folder_id = folder_id or os.environ.get("GDRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        print("[!] 未設定 GDRIVE_FOLDER_ID，無法下載雲端檔案。")
        return []

    service = get_gdrive_service()
    if not service:
        print("[!] 無法取得 Google Drive 服務認證，請確認 GDRIVE_SERVICE_ACCOUNT_KEY 設定。")
        return []

    os.makedirs(dest_dir, exist_ok=True)

    print(f"[*] 正在連線 Google Drive 資料夾 (ID: {folder_id})...")
    files = list_gdrive_parquet_files(service, folder_id)
    if not files:
        print(f"[!] Google Drive 資料夾內未找到符合條件的 Parquet 檔案。")
        return []

    # 嚴格只挑選券商分點檔案 (api_absr1)，排除 close1、margin、taifex、tdcc 等輔助檔案，避免按日期去重時排擠分點檔案
    broker_files = [
        f for f in files 
        if "absr1" in f["name"] and "close1" not in f["name"] and "margin" not in f["name"] and "taifex" not in f["name"] and "tdcc" not in f["name"]
    ]
    if not broker_files:
        # 若無明確 absr1，退回排除 close1 的候選清單
        broker_files = [f for f in files if "close1" not in f["name"]]

    # 取最近 N 個不重複日期的分點檔案
    seen_dates = set()
    target_files = []
    for f in broker_files:
        if f["date"] not in seen_dates:
            seen_dates.add(f["date"])
            target_files.append(f)
            if len(seen_dates) >= lookback_days:
                break

    downloaded_paths = []
    print(f"[*] 準備下載最近 {len(target_files)} 個交易日分點數據 (天期設定: {lookback_days} 日)...")

    for tf in target_files:
        local_path = os.path.join(dest_dir, tf["name"])
        # 若本機已存在且大小一致則略過重複下載
        if os.path.exists(local_path) and os.path.getsize(local_path) == tf["size"] and tf["size"] > 0:
            print(f"[✓] 本地快取命中: {tf['name']} (大小: {tf['size'] / 1024 / 1024:.2f} MB)")
            downloaded_paths.append(local_path)
            continue

        print(f"[*] 正在下載: {tf['name']} ({tf['size'] / 1024 / 1024:.2f} MB, 日期: {tf['date']})...")
        request = service.files().get_media(fileId=tf["id"])
        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024 * 5)
            done = False
            while not done:
                status, done = downloader.next_chunk()

        print(f"[✓] 下載完成: {tf['name']}")
        downloaded_paths.append(local_path)

    return downloaded_paths


def download_recent_close_price_files(
    folder_id: Optional[str] = None,
    lookback_days: int = 60,
    dest_dir: str = "./cloud_data_close"
) -> List[str]:
    """
    下載 Google Drive 雲端中最近 N 個交易日的每日收盤價 Parquet 檔案 (api_close1_*)
    獨立於 download_recent_parquet_files，避免共用同一份按日期去重清單而互相排擠
    回傳本地下載後的檔案路徑列表
    """
    from googleapiclient.http import MediaIoBaseDownload

    folder_id = folder_id or os.environ.get("GDRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        print("[!] 未設定 GDRIVE_FOLDER_ID，無法下載雲端收盤價檔案。")
        return []

    service = get_gdrive_service()
    if not service:
        print("[!] 無法取得 Google Drive 服務認證，請確認 GDRIVE_SERVICE_ACCOUNT_KEY 設定。")
        return []

    os.makedirs(dest_dir, exist_ok=True)

    all_files = list_gdrive_parquet_files(service, folder_id)
    close_files = [f for f in all_files if "close1" in f["name"]]
    if not close_files:
        print("[!] Google Drive 資料夾內未找到 api_close1 收盤價檔案。")
        return []

    seen_dates = set()
    target_files = []
    for f in close_files:
        if f["date"] not in seen_dates:
            seen_dates.add(f["date"])
            target_files.append(f)
            if len(seen_dates) >= lookback_days:
                break

    downloaded_paths = []
    print(f"[*] 準備下載最近 {len(target_files)} 個交易日收盤價數據...")

    for tf in target_files:
        local_path = os.path.join(dest_dir, tf["name"])
        if os.path.exists(local_path) and os.path.getsize(local_path) == tf["size"] and tf["size"] > 0:
            downloaded_paths.append(local_path)
            continue

        request = service.files().get_media(fileId=tf["id"])
        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024 * 5)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        downloaded_paths.append(local_path)

    print(f"[✓] 收盤價檔案下載完成，共 {len(downloaded_paths)} 個檔案就緒。")
    return downloaded_paths


def download_recent_files_by_pattern(
    pattern: str,
    folder_id: Optional[str] = None,
    lookback_days: int = 60,
    dest_dir: str = "./cloud_data_custom"
) -> List[str]:
    """
    下載 Google Drive 雲端中符合指定 pattern (如 'margin', 'taifex', 'tdcc') 最近 N 個交易日的 Parquet 檔案
    """
    from googleapiclient.http import MediaIoBaseDownload

    folder_id = folder_id or os.environ.get("GDRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        return []

    service = get_gdrive_service()
    if not service:
        return []

    os.makedirs(dest_dir, exist_ok=True)

    all_files = list_gdrive_parquet_files(service, folder_id)
    matched_files = [f for f in all_files if pattern.lower() in f["name"].lower()]
    if not matched_files:
        return []

    seen_dates = set()
    target_files = []
    for f in matched_files:
        if f["date"] not in seen_dates:
            seen_dates.add(f["date"])
            target_files.append(f)
            if len(seen_dates) >= lookback_days:
                break

    downloaded_paths = []
    for tf in target_files:
        local_path = os.path.join(dest_dir, tf["name"])
        if os.path.exists(local_path) and os.path.getsize(local_path) == tf["size"] and tf["size"] > 0:
            downloaded_paths.append(local_path)
            continue

        try:
            request = service.files().get_media(fileId=tf["id"])
            with io.FileIO(local_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024 * 5)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
            downloaded_paths.append(local_path)
        except Exception as e:
            print(f"[!] 下載 {tf['name']} 失敗: {e}")

    print(f"[✓] {pattern} 檔案下載完成，共 {len(downloaded_paths)} 個檔案就緒。")
    return downloaded_paths


if __name__ == "__main__":
    test_days = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    paths = download_recent_parquet_files(lookback_days=test_days)
    print(f"[*] 下載完成，共有 {len(paths)} 個檔案就緒：", paths)
