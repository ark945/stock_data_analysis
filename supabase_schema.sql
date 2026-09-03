-- ==============================================================================
-- myStock 雲端籌碼戰情室 (Chip Intelligence War Room) - Supabase 資料庫建表腳本
-- 請至 Supabase Dashboard -> SQL Editor 貼上並執行本腳本
-- ==============================================================================

-- 1. 每日大盤主力多空速覽表
CREATE TABLE IF NOT EXISTS daily_chip_summary (
    trade_date DATE PRIMARY KEY,
    bull_champion_broker TEXT,          -- 今日多頭司令分點 (如 國泰-敦南)
    bull_champion_amt NUMERIC,          -- 買超金額 (億元)
    bull_champion_stocks TEXT,          -- 核心買超標的 (如 台積電(+10.0億)、台達電(+6.9億))
    bear_champion_broker TEXT,          -- 今日空頭調節分點 (如 摩根大通)
    bear_champion_amt NUMERIC,          -- 賣超金額 (億元)
    bear_champion_stocks TEXT,          -- 核心調節標的
    market_sentiment TEXT,              -- 多空氛圍 (偏多 / 震盪 / 偏空)
    total_signals_count INT,            -- 今日觸發訊號總數
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. 主力四週期吸籌總表 (5 / 10 / 20 / 60日)
CREATE TABLE IF NOT EXISTS chip_accumulation_signals (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    period_days INT NOT NULL,           -- 吸籌週期：5 (短線點火) / 10 (雙週波段) / 20 (月波段川湖) / 60 (季線長莊)
    symbol VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    market VARCHAR(10) NOT NULL,        -- 上市 / 上櫃 / 興櫃
    broker_id VARCHAR(10),
    broker_name VARCHAR(50) NOT NULL,   -- 主力分點名稱 (如 富邦-新店、台灣摩根士丹利)
    net_amt_yi NUMERIC NOT NULL,        -- 週期累計淨買超金額 (億元)
    net_vol_sheets NUMERIC NOT NULL,    -- 週期累計淨買超張數 (張)
    buy_avg_price NUMERIC,              -- 主力加權買進成本均價
    close_price NUMERIC,                -- 最新收盤價
    cost_deviation_pct NUMERIC,         -- 成本偏離度%
    buy_purity_pct NUMERIC,             -- 買進純度%
    concentration_pct NUMERIC,          -- 分點買盤集中度%
    backtest_win_rate NUMERIC,          -- 該標的歷史訊號回測勝率%
    backtest_avg_return_pct NUMERIC,    -- 該標的歷史訊號平均報酬率%
    persona_tag VARCHAR(50),            -- 主力標籤 (波段長莊 / 隔日沖 / 本土法人)
    action_guide TEXT,                  -- 次日實戰作戰指引
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_accum_signal UNIQUE (trade_date, period_days, symbol, broker_name)
);

CREATE INDEX IF NOT EXISTS idx_accum_date_period ON chip_accumulation_signals(trade_date, period_days);
CREATE INDEX IF NOT EXISTS idx_accum_symbol ON chip_accumulation_signals(symbol);

-- 3. 主力出貨/逃離與散戶接盤下車表
CREATE TABLE IF NOT EXISTS chip_exit_signals (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    exit_type VARCHAR(30) NOT NULL,     -- 週期型態：5d出貨 / 10d調節 / 20d出清
    symbol VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    market VARCHAR(10) NOT NULL,
    dump_broker_name VARCHAR(50) NOT NULL,  -- 出貨大戶分點名稱 (如 凱基-台北)
    dump_amt_yi NUMERIC NOT NULL,       -- 淨賣超金額 (億元)
    dump_vol_sheets NUMERIC NOT NULL,   -- 淨賣超張數 (張)
    sell_avg_price NUMERIC,             -- 大戶出貨加權均價
    close_price NUMERIC,                -- 最新收盤價
    retail_broker_name VARCHAR(50),     -- 主要接盤之散戶券商分點
    warning_level VARCHAR(20),          -- 預警等級 (🚨 高危險出貨 / ⚠️ 震盪調節)
    action_guide TEXT,                  -- 防禦與停損指引
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_exit_signal UNIQUE (trade_date, exit_type, symbol, dump_broker_name)
);

CREATE INDEX IF NOT EXISTS idx_exit_date ON chip_exit_signals(trade_date);
CREATE INDEX IF NOT EXISTS idx_exit_symbol ON chip_exit_signals(symbol);

-- 4. 外資席位與本土法人部重押表
CREATE TABLE IF NOT EXISTS broker_institution_ranks (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    category VARCHAR(20) NOT NULL,      -- 'FOREIGN' (外資) 或 'DOMESTIC_INST' (本土法人)
    broker_name VARCHAR(50) NOT NULL,   -- 券商席位名稱 (如 台灣摩根士丹利、國票-敦北法人)
    symbol VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    market VARCHAR(10) NOT NULL,
    net_amt_yi NUMERIC NOT NULL,
    net_sheets NUMERIC NOT NULL,
    buy_avg_price NUMERIC,
    buy_purity_pct NUMERIC,
    feature_tag VARCHAR(50),            -- 屬性標籤 (如 💎 波段機構主力 / 🎯 絕對鎖碼)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_inst_rank UNIQUE (trade_date, category, broker_name, symbol)
);

CREATE INDEX IF NOT EXISTS idx_inst_date ON broker_institution_ranks(trade_date);

-- 5. 尾盤放量站上 VWAP 歸因表
CREATE TABLE IF NOT EXISTS vwap_attribution_signals (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    market VARCHAR(10) NOT NULL,
    close_price NUMERIC NOT NULL,
    vwap_price NUMERIC NOT NULL,
    vwap_premium_pct NUMERIC NOT NULL,  -- 溢價%
    broker_name VARCHAR(50) NOT NULL,   -- 尾盤推手分點
    broker_buy_avg NUMERIC NOT NULL,    -- 分點均價
    net_amt_yi NUMERIC NOT NULL,
    net_vol_sheets NUMERIC NOT NULL,
    buy_purity_pct NUMERIC NOT NULL,
    persona_tag VARCHAR(50),            -- 隔日沖急拉 / 波段主力高位鎖碼
    action_guide TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_vwap_signal UNIQUE (trade_date, symbol, broker_name)
);

CREATE INDEX IF NOT EXISTS idx_vwap_date ON vwap_attribution_signals(trade_date);

-- ==============================================================================
-- 安全與權限設定 (RLS: Row Level Security)
-- 允許 myStock 前端匿名/登入唯讀查詢，後端服務金鑰寫入
-- ==============================================================================
ALTER TABLE daily_chip_summary ENABLE ROW LEVEL SECURITY;
ALTER TABLE chip_accumulation_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE chip_exit_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE broker_institution_ranks ENABLE ROW LEVEL SECURITY;
ALTER TABLE vwap_attribution_signals ENABLE ROW LEVEL SECURITY;

-- 允許任何人 (前端網頁) 唯讀查詢
DROP POLICY IF EXISTS "Public Read daily_chip_summary" ON daily_chip_summary;
CREATE POLICY "Public Read daily_chip_summary" ON daily_chip_summary FOR SELECT USING (true);

DROP POLICY IF EXISTS "Public Read chip_accumulation_signals" ON chip_accumulation_signals;
CREATE POLICY "Public Read chip_accumulation_signals" ON chip_accumulation_signals FOR SELECT USING (true);

DROP POLICY IF EXISTS "Public Read chip_exit_signals" ON chip_exit_signals;
CREATE POLICY "Public Read chip_exit_signals" ON chip_exit_signals FOR SELECT USING (true);

DROP POLICY IF EXISTS "Public Read broker_institution_ranks" ON broker_institution_ranks;
CREATE POLICY "Public Read broker_institution_ranks" ON broker_institution_ranks FOR SELECT USING (true);

DROP POLICY IF EXISTS "Public Read vwap_attribution_signals" ON vwap_attribution_signals;
CREATE POLICY "Public Read vwap_attribution_signals" ON vwap_attribution_signals FOR SELECT USING (true);
