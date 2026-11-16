-- Project 4: Athena table over the processed layer (design doc 4.2, 4.8).
-- Run in the Athena query editor, workgroup "daily-prices".
-- Replace YOUR_DATA_BUCKET with the data bucket name before running.
--
-- The schema is the daily_prices contract: it changes only together with the
-- Glue job (src/raw_to_processed.py) and its tests. No crawler (decision D2).
-- The table is not partitioned (D5), so no partition registration is needed.

CREATE DATABASE IF NOT EXISTS financial_data;

CREATE EXTERNAL TABLE IF NOT EXISTS financial_data.daily_prices (
    `ticker`        string        COMMENT 'Stock symbol, from Meta Data / 2. Symbol',
    `trade_date`    date          COMMENT 'Trading day on the New York exchange (US/Eastern)',
    `open`          decimal(12,4),
    `high`          decimal(12,4),
    `low`           decimal(12,4),
    `close`         decimal(12,4),
    `volume`        bigint,
    `snapshot_date` date          COMMENT 'Run day (UTC) of the raw file the row comes from'
)
COMMENT 'One row per ticker and trading day, latest snapshot wins'
STORED AS PARQUET
LOCATION 's3://YOUR_DATA_BUCKET/processed/daily_prices/';
