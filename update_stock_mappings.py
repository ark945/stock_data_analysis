# -*- coding: utf-8 -*-
"""
全市場股票代碼、中文名稱與市場類別 (上市/上櫃/興櫃) 對照建置模組
==================================================================
資料來源：
1. 台灣證券交易所 (TWSE) 官方 OpenAPI (上市行情與上市公司基本資料)
2. 證券櫃檯買賣中心 (TPEx) 官方 OpenAPI (上櫃行情與上櫃公司基本資料)
3. 證券期貨局 ISIN 證券編碼公告清單 (全市場上市/上櫃/興櫃，以 CP950 精準解碼)
4. 歷史/興櫃/下市櫃/特殊標的補正清單 (確保 4546 長亨、3644 凌嘉科 等永久正確)
"""

import os
import sys
import json
import re
import requests
from bs4 import BeautifulSoup

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 歷史/興櫃/特殊股票補正字典 (確保下市櫃、撤銷公開發行或特殊標的 100% 正確)
OVERRIDE_STOCKS = {
    "4546": {"name": "長亨", "market": "興櫃"},
    "3644": {"name": "凌嘉科", "market": "興櫃"},
    "2059": {"name": "川湖", "market": "上市"},
    "2330": {"name": "台積電", "market": "上市"},
    "6531": {"name": "愛普*", "market": "上櫃"},
    "3017": {"name": "奇鋐", "market": "上市"},
    "8069": {"name": "元太", "market": "上櫃"},
    "3293": {"name": "鈊象", "market": "上櫃"},
    "0050": {"name": "元大台灣50", "market": "上市"},
    "0056": {"name": "元大高股息", "market": "上市"},
    "00878": {"name": "國泰永續高股息", "market": "上市"},
    "00919": {"name": "群益台灣精選高息", "market": "上市"},
    "00929": {"name": "復華台灣科技優息", "market": "上市"},
    "00940": {"name": "元大台灣價值高息", "market": "上市"},
}


def build_stock_mappings(cache_dir: str = None) -> tuple[dict, dict]:
    """
    爬取並整合 TWSE、TPEx 及 ISIN 資料，建立完整股票名稱與市場對照字典
    回傳 (stock_names_dict, stock_markets_dict)
    """
    if cache_dir is None:
        cache_dir = os.path.dirname(__file__)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    stock_names = {}
    stock_markets = {}

    # 1. 先填入基礎覆蓋字典
    for code, info in OVERRIDE_STOCKS.items():
        stock_names[code] = info["name"]
        stock_markets[code] = info["market"]

    # 2. TWSE OpenAPI 上市每日行情
    try:
        r = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", headers=headers, timeout=6)
        if r.status_code == 200:
            for d in r.json():
                c = str(d.get("Code", "")).strip()
                n = str(d.get("Name", "")).strip()
                if c and n:
                    stock_names[c] = n
                    stock_markets[c] = "上市"
    except Exception as e:
        print(f"[!] TWSE STOCK_DAY_ALL 載入略過: {e}")

    # 3. TWSE OpenAPI 上市公司基本資料
    try:
        r = requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap03_L", headers=headers, timeout=6)
        if r.status_code == 200:
            for d in r.json():
                c = str(d.get("公司代號", "")).strip()
                n = str(d.get("公司簡稱", "")).strip()
                if c and n:
                    if c not in stock_names:
                        stock_names[c] = n
                    if c not in stock_markets:
                        stock_markets[c] = "上市"
    except Exception as e:
        print(f"[!] TWSE t187ap03_L 載入略過: {e}")

    # 4. TPEx OpenAPI 上櫃每日收盤行情
    try:
        r = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes", headers=headers, timeout=6)
        if r.status_code == 200:
            for d in r.json():
                c = str(d.get("SecuritiesCompanyCode", "")).strip()
                n = str(d.get("CompanyName", "")).strip()
                if c and n:
                    stock_names[c] = n
                    stock_markets[c] = "上櫃"
    except Exception as e:
        print(f"[!] TPEx 每日行情載入略過: {e}")

    # 5. TPEx OpenAPI 上櫃公司基本資料
    try:
        r = requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O", headers=headers, timeout=6)
        if r.status_code == 200:
            for d in r.json():
                c = str(d.get("SecuritiesCompanyCode", "")).strip()
                n = str(d.get("CompanyAbbreviation", "")).strip() or str(d.get("CompanyName", "")).strip()
                if c and n:
                    if c not in stock_names:
                        stock_names[c] = n
                    if c not in stock_markets:
                        stock_markets[c] = "上櫃"
    except Exception as e:
        print(f"[!] TPEx t187ap03_O 載入略過: {e}")

    # 6. ISIN 網頁 (Mode 2:上市, Mode 4:上櫃, Mode 5:興櫃) - 使用 CP950 正確解碼
    isin_modes = [(2, "上市"), (4, "上櫃"), (5, "興櫃")]
    for mode, market_type in isin_modes:
        try:
            url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}"
            r = requests.get(url, headers=headers, timeout=8)
            r.encoding = "cp950"
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                for tr in soup.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        t0 = tds[0].text.strip()
                        parts = re.split(r"[\s\u3000]+", t0)
                        if len(parts) >= 2:
                            c, n = parts[0].strip(), parts[1].strip()
                            if c and n and len(c) <= 6:
                                if c not in stock_names or not stock_names[c]:
                                    stock_names[c] = n
                                if c not in stock_markets:
                                    stock_markets[c] = market_type
        except Exception as e:
            print(f"[!] ISIN Mode {mode} ({market_type}) 載入略過: {e}")

    # 7. 再次確認覆蓋清單
    for code, info in OVERRIDE_STOCKS.items():
        stock_names[code] = info["name"]
        stock_markets[code] = info["market"]

    # 儲存快取檔案
    name_cache_path = os.path.join(cache_dir, "stock_name_map.json")
    market_cache_path = os.path.join(cache_dir, "stock_market_map.json")

    try:
        with open(name_cache_path, "w", encoding="utf-8") as f:
            json.dump(stock_names, f, ensure_ascii=False, indent=2)
        with open(market_cache_path, "w", encoding="utf-8") as f:
            json.dump(stock_markets, f, ensure_ascii=False, indent=2)
        print(f"[OK] 成功更新股票字典快取！共計 {len(stock_names):,} 檔標的")
    except Exception as e:
        print(f"[!] 寫入快取檔失敗: {e}")

    return stock_names, stock_markets


if __name__ == "__main__":
    names, markets = build_stock_mappings()
    print("=" * 50)
    print("驗證重點標的：")
    for test_c in ["2330", "2059", "4546", "3644", "6531", "0050", "3017", "8069", "3293"]:
        print(f"  {test_c} -> {names.get(test_c)} ({markets.get(test_c, '未知')})")
    print("=" * 50)
