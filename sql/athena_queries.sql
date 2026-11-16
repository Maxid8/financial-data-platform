-- Project 4: business questions over the processed layer (design doc 4.8).
-- Athena (Trino SQL), workgroup "daily-prices", database financial_data.
-- Prices are stored as DECIMAL(12,4); calculations cast to DOUBLE, where
-- an approximate result is acceptable (returns, volatility).
-- Run one query at a time: the Athena query editor runs one statement per run.


-- Q1. Latest trading day: close and daily return per ticker.
WITH returns AS (
    SELECT
        ticker,
        trade_date,
        close,
        LAG(close) OVER (PARTITION BY ticker ORDER BY trade_date) AS previous_close
    FROM financial_data.daily_prices
)
SELECT
    ticker,
    trade_date,
    close,
    ROUND(CAST(close AS DOUBLE) / CAST(previous_close AS DOUBLE) - 1, 4) AS daily_return
FROM returns
WHERE trade_date = (SELECT MAX(trade_date) FROM financial_data.daily_prices)
ORDER BY ticker;


-- Q2. 20-day moving average on the latest trading day, and the distance of
--     the close from it (positive: trading above its recent average).
WITH averages AS (
    SELECT
        ticker,
        trade_date,
        close,
        AVG(CAST(close AS DOUBLE)) OVER (
            PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma_20,
        COUNT(*) OVER (
            PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS days_in_window
    FROM financial_data.daily_prices
)
SELECT
    ticker,
    trade_date,
    close,
    ROUND(ma_20, 4) AS ma_20,
    ROUND(CAST(close AS DOUBLE) / ma_20 - 1, 4) AS distance_from_ma_20
FROM averages
WHERE trade_date = (SELECT MAX(trade_date) FROM financial_data.daily_prices)
  AND days_in_window = 20
ORDER BY ticker;


-- Q3. Volatility: annualised standard deviation of daily log returns over the
--     last 60 trading days per ticker (252 trading days per year).
WITH returns AS (
    SELECT
        ticker,
        trade_date,
        LN(CAST(close AS DOUBLE) / CAST(LAG(close) OVER (PARTITION BY ticker ORDER BY trade_date) AS DOUBLE)) AS log_return,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS days_back
    FROM financial_data.daily_prices
)
SELECT
    ticker,
    COUNT(log_return) AS return_days,
    ROUND(STDDEV_SAMP(log_return) * SQRT(252), 4) AS annualised_volatility
FROM returns
WHERE days_back <= 60
GROUP BY ticker
ORDER BY annualised_volatility DESC;


-- Q4. Maximum drawdown: the largest fall from a previous peak close, per ticker,
--     with the day it was reached.
WITH running AS (
    SELECT
        ticker,
        trade_date,
        close,
        MAX(close) OVER (PARTITION BY ticker ORDER BY trade_date ROWS UNBOUNDED PRECEDING) AS peak_close
    FROM financial_data.daily_prices
),
drawdowns AS (
    SELECT
        ticker,
        trade_date,
        CAST(close AS DOUBLE) / CAST(peak_close AS DOUBLE) - 1 AS drawdown
    FROM running
)
SELECT
    ticker,
    ROUND(MIN(drawdown), 4) AS max_drawdown,
    MIN_BY(trade_date, drawdown) AS trough_date
FROM drawdowns
GROUP BY ticker
ORDER BY max_drawdown;


-- Q5. Monthly return per ticker: first and last close of each calendar month.
SELECT
    ticker,
    DATE_TRUNC('month', trade_date) AS month,
    MIN_BY(close, trade_date) AS first_close,
    MAX_BY(close, trade_date) AS last_close,
    ROUND(CAST(MAX_BY(close, trade_date) AS DOUBLE) / CAST(MIN_BY(close, trade_date) AS DOUBLE) - 1, 4) AS monthly_return,
    COUNT(*) AS trading_days
FROM financial_data.daily_prices
GROUP BY ticker, DATE_TRUNC('month', trade_date)
ORDER BY ticker, month;


-- Q6. Unusual trading volume: days with more than twice the average volume of
--     the previous 20 trading days.
WITH volumes AS (
    SELECT
        ticker,
        trade_date,
        volume,
        AVG(CAST(volume AS DOUBLE)) OVER (
            PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_volume_20,
        COUNT(*) OVER (
            PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS days_in_window
    FROM financial_data.daily_prices
)
SELECT
    ticker,
    trade_date,
    volume,
    ROUND(volume / avg_volume_20, 2) AS volume_ratio
FROM volumes
WHERE days_in_window = 20
  AND volume > 2 * avg_volume_20
ORDER BY volume_ratio DESC;
