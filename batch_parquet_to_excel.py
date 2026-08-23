import os
import sys
import glob
import time
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

def convert_single_file(parquet_path: str):
    excel_path = os.path.splitext(parquet_path)[0] + ".xlsx"
    filename = os.path.basename(parquet_path)
    
    # 若已存在且大小完整 (> 20MB) 則略過
    if os.path.exists(excel_path) and os.path.getsize(excel_path) > 20 * 1024 * 1024:
        return (filename, True, 0, "已存在完整檔案，略過")
    
    start_t = time.time()
    try:
        df = pd.read_parquet(parquet_path)
        rows, cols = df.shape
        
        # 使用 openpyxl 確保所有欄位完整寫入
        df.to_excel(excel_path, engine="openpyxl", index=False)
            
        elapsed = time.time() - start_t
        size_mb = os.path.getsize(excel_path) / (1024 * 1024)
        return (filename, True, elapsed, f"{rows:,} 列, {size_mb:.1f} MB, {elapsed:.1f}s")
    except Exception as e:
        return (filename, False, time.time() - start_t, str(e))

def main():
    target_dir = os.path.join(os.path.dirname(__file__), "20260822分點資料")
    if len(sys.argv) > 1:
        target_dir = sys.argv[1]
        
    parquet_files = sorted(glob.glob(os.path.join(target_dir, "*.parquet")))
    total_files = len(parquet_files)
    
    # 篩選未轉換或之前轉失敗 (< 20MB) 的檔案
    to_convert = []
    for f in parquet_files:
        out_f = os.path.splitext(f)[0] + ".xlsx"
        if not (os.path.exists(out_f) and os.path.getsize(out_f) > 20 * 1024 * 1024):
            to_convert.append(f)
            
    print(f"==================================================")
    print(f"[*] 找到 Parquet 總數: {total_files} 個")
    print(f"[*] 已存在完整 Excel: {total_files - len(to_convert)} 個")
    print(f"[*] 待轉換 (包含先前失敗重轉): {len(to_convert)} 個")
    print(f"[*] 啟用多行程並行轉檔 (Worker 數: 4)...")
    print(f"==================================================")
    sys.stdout.flush()
    
    if not to_convert:
        print("[OK] 所有檔案皆已轉換完成！")
        return

    overall_start = time.time()
    completed_count = total_files - len(to_convert)
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        future_to_file = {executor.submit(convert_single_file, f): f for f in to_convert}
        
        for future in as_completed(future_to_file):
            filename, success, elapsed, msg = future.result()
            completed_count += 1
            status_tag = "[OK]" if success else "[FAIL]"
            print(f"[{completed_count}/{total_files}] {status_tag} {filename} -> {msg}")
            sys.stdout.flush()

    total_time = time.time() - overall_start
    print(f"==================================================")
    print(f"[OK] 全部轉換完成！總耗時: {total_time:.1f} 秒 ({total_time/60:.1f} 分鐘)")
    print(f"==================================================")

if __name__ == "__main__":
    main()
