-- ============================================
-- Stock Analytics Pipeline
-- schema.sql: Create all tables
-- ============================================

-- Drop tables if they already exist (useful for resetting)
DROP TABLE IF EXISTS stock_prices;
DROP TABLE IF EXISTS stock_signals;

-- ============================================
-- TABLE 1: Raw stock price data
-- One row per stock per trading day
-- ============================================
CREATE TABLE stock_prices (
    id            SERIAL PRIMARY KEY,
    ticker        VARCHAR(10)    NOT NULL,
    trade_date    DATE           NOT NULL,
    open_price    NUMERIC(10,2),
    high_price    NUMERIC(10,2),
    low_price     NUMERIC(10,2),
    close_price   NUMERIC(10,2),
    volume        BIGINT,
    UNIQUE(ticker, trade_date)
);

-- ============================================
-- TABLE 2: Computed signals (filled by Python)
-- One row per stock per trading day
-- ============================================
CREATE TABLE stock_signals (
    id                  SERIAL PRIMARY KEY,
    ticker              VARCHAR(10)    NOT NULL,
    trade_date          DATE           NOT NULL,
    daily_return_pct    NUMERIC(10,4),
    volatility_pct      NUMERIC(10,4),
    moving_avg_30d      NUMERIC(10,2),
    moving_avg_7d       NUMERIC(10,2),
    UNIQUE(ticker, trade_date)
);

-- Indexes to make queries faster
CREATE INDEX idx_stock_prices_ticker ON stock_prices(ticker);
CREATE INDEX idx_stock_prices_date ON stock_prices(trade_date);
CREATE INDEX idx_stock_signals_ticker ON stock_signals(ticker);