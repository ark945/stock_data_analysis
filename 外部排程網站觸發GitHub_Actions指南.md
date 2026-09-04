# 外部排程網站 (Cron-job.org) 精準觸發 GitHub Actions 分析日報完整指南

本指南專為 **主力重押日報分析系統 (`stock_data_analysis`)** 設計，指導如何使用免費且極度穩定的外部排程平台（以 [Cron-job.org](https://cron-job.org/) 為例），透過 GitHub REST API 以 **秒級精準度** 自動觸發主力重押日報分析與 Email/Telegram 報告派發。

---

## 🎯 為什麼使用外部排程？

1. **秒級精準點火**：徹底解決 GitHub 內建 `schedule (cron)` 常態性延遲 15~40 分鐘或高峰期被丟包的問題。
2. **完美時序銜接**：可設定在每日爬蟲完成後（例如每晚 **20:00** 或 **20:15**），準時發動主力重押分析並派發精美 HTML 報告。
3. **靈活管理**：隨時可以在 Web 介面上啟用、暫停或手動測試執行。

---

## 🔑 準備工作：取得 GitHub 個人存取權杖 (Personal Access Token)

若您之前在爬蟲專案已建立過 Token，可直接沿用；若需新建，請依下列步驟：

1. 登入 GitHub，點擊右上角個人頭像 ➔ **Settings**。
2. 滾動至左側最下方，點擊 **Developer Settings** ➔ **Personal access tokens** ➔ **Tokens (classic)**。
3. 點擊右上角 **Generate new token** ➔ **Generate new token (classic)**。
4. 設定參數：
   - **Note (備註名稱)**：`Cron-Job Trigger Analysis`
   - **Expiration (過期時間)**：建議選擇 `No expiration` (無期限) 或 `90 days`
   - **Select scopes (權限勾選)**：務必勾選 **`repo`** (完整存取儲存庫權限) 及 **`workflow`** (更新與觸發工作流程)
5. 點擊最下方綠色按鈕 **Generate token**。
6. **複製並妥善保存 Token**（格式為 `ghp_xxxxxxxxxxxxxxxxxxxx`）。

---

## 🚀 Cron-job.org 設定步驟

### 步驟一：註冊與建立 Job
1. 前往 [Cron-job.org](https://cron-job.org/) 註冊並登入帳號。
2. 進入後台 Dashboard，點擊右上角 **`CREATE CRONJOB`**。

---

### 步驟二：一般設定 (General)

* **Title (標題)**：`每日主力重押日報分析 (stock_data_analysis)`
* **URL (目標網址)**：
  ```text
  https://api.github.com/repos/ark945/stock_data_analysis/actions/workflows/daily_heavy_accumulation_report.yml/dispatches
  ```
* **Execution Schedule (排程時間)**：
  - **時區 (Timezone)**：選擇 `Asia/Taipei` (台灣時區)
  - **頻率**：選擇每週一至週五 (週一、二、三、四、五)
  - **時間**：建議設為晚上 **`20:07`** 或 **`20:15`**（在爬蟲完成全市場資料上傳之後）

---

### 步驟三：進階請求設定 (Advanced / Request Method)

點開下方 **Advanced** 或 **Request** 設定區：

1. **Request Method**：選擇 **`POST`**
2. **Request Headers (自訂標頭，務必設定 5 條)**：
   點擊 `＋ 新增 (Add header)` 依序填入：

   | Header Key (鍵) | Header Value (值) | 說明 |
   | :--- | :--- | :--- |
   | `Accept` | `application/vnd.github+json` | GitHub API 規範格式 |
   | `Authorization` | `Bearer ghp_你的GitHub經典Token` | 注意 Bearer 後有半形空格 |
   | `X-GitHub-Api-Version` | `2022-11-28` | GitHub API 版本 |
   | `User-Agent` | `CronJob-Trigger-Bot` | GitHub 要求必須提供 UA |
   | `Content-Type` | `application/json` | 指定 Body 為 JSON 格式 |

3. **Request Body (請求本體)**：
   - 選擇 **`Raw data`** 或 **`JSON`**，貼入以下內容：
   ```json
   {
     "ref": "main"
   }
   ```

---

### ⚡ 懶人一鍵匯入方式 (IMPORT FROM CURL)

如果您使用的是 Cron-job.org 的 **`IMPORT FROM CURL`** 功能，可以直接複製以下指令並將 `ghp_你的GitHubToken` 替換為您的實際 Token：

```bash
curl -X POST https://api.github.com/repos/ark945/stock_data_analysis/actions/workflows/daily_heavy_accumulation_report.yml/dispatches \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ghp_你的GitHubToken" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  -H "User-Agent: CronJob-Trigger-Bot" \
  -H "Content-Type: application/json" \
  -d '{"ref":"main"}'
```

---

### 步驟四：測試與驗證

1. 設定完成後，先點擊介面上的 **`TEST RUN` (測試執行)**。
2. 若回傳 **`204 No Content`** 或 **`Success`**，代表已成功觸發 GitHub Actions！
3. 前往 GitHub 的 [stock_data_analysis Actions 頁面](https://github.com/ark945/stock_data_analysis/actions)，即可看到工作流程已經立即啟動運行。
4. 確認無誤後，點擊 **`CREATE` / `SAVE`** 儲存排程。

---

## 📅 排程建議時間對照表 (完整 5 大核心排程)

| 專案 | 任務類型 | 建議觸發時間 (台灣時間) | 說明與效益 |
| :--- | :--- | :--- | :--- |
| **爬蟲專案 (`stock_data_downloader`)** | ① 平日全市場分點爬蟲 | 每週一至週五 `17:35` | 盤後自動爬取 TWSE + TPEx 全市場分點買賣日報並上傳 GDrive |
| **爬蟲專案 (`stock_data_downloader`)** | ② 清晨備援補抓 | 每週二至週六 `05:31` | 自動檢查若前一日有漏網股票則進行二次補抓 |
| **爬蟲專案 (`stock_data_downloader`)** | ③ 週六集保大戶分散表 | 每週六 `09:30` | 官方通常於 08:30~09:00 發布當週資料，秒級自動下載並上傳 GDrive |
| **分析專案 (`stock_data_analysis`)** | ④ 平日主力重押日報 | 每週一至週五 `20:05` | 計算 4 週期主力吸籌、出貨雷達、同步 Supabase 並寄發 Email |
| **分析專案 (`stock_data_analysis`)** | ⑤ 週末特刊 / 看板同步 | 每週六 `10:00` | 讀取最新集保數據，重算千張大戶持股%與週增減，全面更新 myStock 戰情室 |

### 💡 週六任務 ⑤ 的 Request Body 設定說明
在 Cron-job.org 建立週六 10:00 的分析任務時：
- **目標 URL**：`https://api.github.com/repos/ark945/stock_data_analysis/actions/workflows/daily_heavy_accumulation_report.yml/dispatches`
- **Method**：`POST`
- **Headers**：填入前述 4 條（含 GitHub PAT Token）
- **Request Body (JSON)**：
  ```json
  {
    "ref": "main",
    "inputs": {
      "target_date": "",
      "send_email": "false"
    }
  }
  ```
  *(說明：傳入 `"send_email": "false"` 可以在週末「靜默更新」myStock 戰情室與 Supabase，不重複寄信打擾收件者；若希望週六早上收到 Email 週末總盤點，將其改為 `"true"` 即可)*
