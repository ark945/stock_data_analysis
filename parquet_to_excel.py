import os
import sys
import time
import pandas as pd

def convert_parquet_to_excel(parquet_path: str, excel_path: str = None):
    if not os.path.exists(parquet_path):
        print(f"錯誤：找不到檔案 {parquet_path}")
        return False

    if excel_path is None:
        excel_path = os.path.splitext(parquet_path)[0] + ".xlsx"

    print(f"[*] 讀取 Parquet 檔案中: {parquet_path}")
    start_time = time.time()
    
    df = pd.read_parquet(parquet_path)
    rows, cols = df.shape
    print(f"[+] 讀取完成！共 {rows:,} 列, {cols} 欄 (耗時 {time.time() - start_time:.2f} 秒)")

    # 檢查 Excel 行數上限 (1,048,576 列，含標題行最多 1,048,575 筆資料)
    EXCEL_LIMIT = 1048575
    if rows > EXCEL_LIMIT:
        print(f"[!] 警告：資料列數 ({rows:,}) 超過單一 Excel 工作表上限 ({EXCEL_LIMIT:,})，將分頁存檔...")
        chunk_size = EXCEL_LIMIT
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            for i in range(0, rows, chunk_size):
                sheet_name = f"Sheet_{(i // chunk_size) + 1}"
                print(f"[*] 寫入工作表 {sheet_name} ({i:,} ~ {min(i + chunk_size, rows):,})...")
                df.iloc[i : i + chunk_size].to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        print(f"[*] 正在輸出至 Excel 檔案: {excel_path} (因資料量約 {rows:,} 筆，請稍候約 1~2 分鐘)...")
        write_start = time.time()
        df.to_excel(excel_path, engine="openpyxl", index=False)
        print(f"[+] 寫入完成！(耗時 {time.time() - write_start:.2f} 秒)")

    file_size_mb = os.path.getsize(excel_path) / (1024 * 1024)
    print(f"[OK] 轉換成功！輸出檔案: {excel_path} (檔案大小: {file_size_mb:.2f} MB)")
    print(f"[OK] 總耗時: {time.time() - start_time:.2f} 秒")
    return True

if __name__ == "__main__":
    default_file = os.path.join(os.path.dirname(__file__), "20260822分點資料", "api_absr1_2026-06-01_2026-06-01.parquet")
    target_parquet = sys.argv[1] if len(sys.argv) > 1 else default_file
    target_excel = sys.argv[2] if len(sys.argv) > 2 else None
    
    convert_parquet_to_excel(target_parquet, target_excel)
