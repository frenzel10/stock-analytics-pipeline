-- ============================================
-- queries.sql
-- Analytical queries for the stock pipeline
-- Run these in psql or a SQL client
-- ============================================

-- ============================================
-- QUERY 1: Price range summary by stock
-- ============================================
SELECT
    ticker,
    MIN(close_price)          AS all_time_low,
    MAX(close_price)          AS all_time_high,
    ROUND(AVG(close_price)::numeric, 2) AS avg_close
FROM stock_prices
GROUP BY ticker
ORDER BY avg_close DESC;

-- ============================================
-- QUERY 2: Annual average closing price
-- ============================================
SELECT
    ticker,
    EXTRACT(YEAR FROM trade_date) AS year,
    ROUND(AVG(close_price)::numeric, 2) AS avg_close
FROM stock_prices
GROUP BY ticker, year
ORDER BY ticker, year;

-- ============================================
-- QUERY 3: Top 20 most volatile single days
-- ============================================
SELECT
    ticker,
    trade_date,
    ROUND(((high_price - low_price) / low_price * 100)::numeric, 2)
        AS volatility_pct
FROM stock_prices
WHERE low_price > 0
ORDER BY volatility_pct DESC
LIMIT 20;

-- ============================================
-- QUERY 4: Average volatility by stock
-- ============================================
SELECT
    ticker,
    ROUND(AVG((high_price - low_price) / low_price * 100)::numeric, 4)
        AS avg_daily_volatility
FROM stock_prices
WHERE low_price > 0
GROUP BY ticker
ORDER BY avg_daily_volatility DESC;

-- ============================================
-- QUERY 5: 30-day moving average (window function)
-- ============================================
SELECT
    ticker,
    trade_date,
    close_price,
    ROUND(AVG(close_price) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )::numeric, 2) AS moving_avg_30d
FROM stock_prices
ORDER BY ticker, trade_date;

-- ============================================
-- QUERY 6: Best and worst year per stock (CTE)
-- ============================================
WITH yearly_returns AS (
    SELECT DISTINCT
        ticker,
        EXTRACT(YEAR FROM trade_date) AS year,
        FIRST_VALUE(close_price) OVER (
            PARTITION BY ticker, EXTRACT(YEAR FROM trade_date)
            ORDER BY trade_date
        ) AS year_open,
        LAST_VALUE(close_price) OVER (
            PARTITION BY ticker, EXTRACT(YEAR FROM trade_date)
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS year_close
    FROM stock_prices
),
annual_perf AS (
    SELECT
        ticker,
        year,
        ROUND(((year_close - year_open) / year_open * 100)::numeric, 2)
            AS annual_return_pct
    FROM yearly_returns
)
SELECT * FROM annual_perf
ORDER BY annual_return_pct DESC;

-- ============================================
-- QUERY 7: Stocks above their 30d moving avg
-- (signal used in real trading strategies)
-- ============================================
SELECT
    sp.ticker,
    sp.trade_date,
    sp.close_price,
    ss.moving_avg_30d,
    CASE
        WHEN sp.close_price > ss.moving_avg_30d THEN 'ABOVE'
        ELSE 'BELOW'
    END AS signal
FROM stock_prices sp
JOIN stock_signals ss
    ON sp.ticker = ss.ticker
    AND sp.trade_date = ss.trade_date
ORDER BY sp.ticker, sp.trade_date;