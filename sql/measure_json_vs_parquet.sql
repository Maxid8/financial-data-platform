-- Project 4: measurement, raw JSON vs processed Parquet (design doc section 8).
-- Athena, workgroup "daily-prices". Run the statements one by one, in order.
-- Replace YOUR_DATA_BUCKET with the data bucket name.
--
-- The raw table exists only for this measurement and is dropped at the end:
-- consumers do not read the raw layer (project 3 contract, decision D2).
-- Dropping an external table removes the catalog entry only, not the S3 data.


-- M1. Temporary table over the raw layer.
--     Maps instead of structs: the JSON keys contain spaces and dots
--     ("2. Symbol", "4. close"), which are awkward as Hive column or field names.
--     case.insensitive=false keeps the original key spelling inside the maps.
CREATE EXTERNAL TABLE financial_data.raw_daily_prices_json (
    meta_data   map<string, string>,
    time_series map<string, map<string, string>>
)
PARTITIONED BY (year int, month int, day int)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'case.insensitive'    = 'false',
    'mapping.meta_data'   = 'Meta Data',
    'mapping.time_series' = 'Time Series (Daily)'
)
LOCATION 's3://YOUR_DATA_BUCKET/raw/';


-- M2. Register the year=/month=/day= folders as partitions.
MSCK REPAIR TABLE financial_data.raw_daily_prices_json;


-- M3. Sanity check: one row per raw file (expected: the "Raw files read" value of the Glue log).
SELECT COUNT(*) AS raw_files
FROM financial_data.raw_daily_prices_json;


-- The business question used for the comparison:
-- "How many trading days and what average close does each ticker have?"

-- Q-PARQUET. On the processed layer.
SELECT
    ticker,
    COUNT(*) AS trading_days,
    ROUND(AVG(close), 4) AS avg_close
FROM financial_data.daily_prices
GROUP BY ticker
ORDER BY ticker;


-- Q-JSON-NAIVE. On the raw layer, without deduplication.
-- Wrong on purpose: every trading day is counted once per snapshot that contains it.
SELECT
    meta_data['2. Symbol'] AS ticker,
    COUNT(*) AS trading_days,
    ROUND(AVG(CAST(t.prices['4. close'] AS DECIMAL(12, 4))), 4) AS avg_close
FROM financial_data.raw_daily_prices_json
CROSS JOIN UNNEST(time_series) AS t (trade_day, prices)
GROUP BY meta_data['2. Symbol']
ORDER BY ticker;


-- Q-JSON-CORRECT. On the raw layer, with the same logic as the Glue job:
-- explode the map, keep the latest snapshot of each (ticker, trade_date).
-- Must return exactly the Q-PARQUET result: an independent check of the job.
WITH exploded AS (
    SELECT
        meta_data['2. Symbol'] AS ticker,
        CAST(t.trade_day AS DATE) AS trade_date,
        CAST(t.prices['4. close'] AS DECIMAL(12, 4)) AS close,
        ROW_NUMBER() OVER (
            PARTITION BY meta_data['2. Symbol'], t.trade_day
            ORDER BY year DESC, month DESC, day DESC
        ) AS snapshot_rank
    FROM financial_data.raw_daily_prices_json
    CROSS JOIN UNNEST(time_series) AS t (trade_day, prices)
)
SELECT
    ticker,
    COUNT(*) AS trading_days,
    ROUND(AVG(close), 4) AS avg_close
FROM exploded
WHERE snapshot_rank = 1
GROUP BY ticker
ORDER BY ticker;


-- M4. Clean up: remove the temporary table (the S3 files stay).
DROP TABLE financial_data.raw_daily_prices_json;
