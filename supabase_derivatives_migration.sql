-- ==============================================================================
-- myStock 籌碼戰情室 - 衍生指標資料庫擴充腳本 (Derivatives & Margin Migration)
-- 包含：融資融券 (券資比/資券增減)、期交所微觀期貨OI (外資大台/散戶小台)、集保千張大戶
-- 請至 Supabase Dashboard -> SQL Editor 貼上並點擊 Run 執行本腳本
-- ==============================================================================

-- 1. 擴充 daily_chip_summary (大盤微觀期權情緒避震器)
ALTER TABLE daily_chip_summary ADD COLUMN IF NOT EXISTS foreign_tx_oi NUMERIC;          -- 外資大台未平倉淨口數
ALTER TABLE daily_chip_summary ADD COLUMN IF NOT EXISTS retail_mtx_ratio_pct NUMERIC;   -- 散戶小台多空比 (%)
ALTER TABLE daily_chip_summary ADD COLUMN IF NOT EXISTS macro_sentiment TEXT;           -- 大盤微觀情緒標籤 (如: ⚠️ 高危誘多 / 🚀 極品軋空)

-- 2. 擴充 chip_accumulation_signals (讓四週期個股卡片直接顯示券資比與千張大戶徽章)
ALTER TABLE chip_accumulation_signals ADD COLUMN IF NOT EXISTS short_margin_ratio_pct NUMERIC; -- 券資比 %
ALTER TABLE chip_accumulation_signals ADD COLUMN IF NOT EXISTS large_shareholder_pct NUMERIC;  -- 千張大戶持股比例 %

-- 3. 新建第 6 張核心表：籌碼衍生指標表 (極品軋空 / 散戶接刀坑 / 籌碼集中度)
CREATE TABLE IF NOT EXISTS chip_derivatives_signals (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    signal_type VARCHAR(30) NOT NULL,    -- 'squeeze' (極品軋空) / 'trap' (散戶接刀) / 'concentrated' (籌碼極度集中)
    symbol VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    market VARCHAR(10) NOT NULL,
    close_price NUMERIC,
    short_margin_ratio_pct NUMERIC,      -- 券資比 %
    margin_net NUMERIC,                  -- 今日融資增減 (張)
    short_net NUMERIC,                   -- 今日融券增減 (張)
    diff_broker_count NUMERIC,           -- 買賣家數差 (買進家數 - 賣出家數)
    large_shareholder_pct NUMERIC,       -- 千張大戶持股比例 %
    retail_shareholder_pct NUMERIC,      -- 50張以下散戶持股比例 %
    persona_tag VARCHAR(50),             -- 戰術屬性標籤 (🚀 極品軋空候選 / ⚠️ 散戶接刀套牢 / 💎 籌碼極度集中)
    action_guide TEXT,                   -- 次日實戰指引
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_deriv_signal UNIQUE (trade_date, signal_type, symbol)
);

CREATE INDEX IF NOT EXISTS idx_deriv_date ON chip_derivatives_signals(trade_date);
CREATE INDEX IF NOT EXISTS idx_deriv_type ON chip_derivatives_signals(signal_type);

-- 4. 安全與權限設定 (RLS: 允許前端公開查詢，後端服務金鑰寫入)
ALTER TABLE chip_derivatives_signals ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Public Read chip_derivatives_signals" ON chip_derivatives_signals;
CREATE POLICY "Public Read chip_derivatives_signals" ON chip_derivatives_signals FOR SELECT USING (true);
