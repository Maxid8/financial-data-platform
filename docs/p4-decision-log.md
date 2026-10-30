# Project 4 - Decision Log (working notes)

Working notes collected during the design phase of project 4 (serverless ETL).
At the end of the project they go into the project 4 design doc, and the
changes to earlier projects go into their own design docs.

## Project 4 design decisions

| # | Topic | Decision |
|---|---|---|
| D1 | Output table | One table, one row per ticker and trading day, key `(ticker, trade_date)`. Prices `DECIMAL(12,4)` (same as `NUMERIC(12,4)` in PostgreSQL), volume `BIGINT`, plus `snapshot_date` (DATE) for lineage. No `processed_at`: with a full rebuild every row would get the same value. |
| D2 | Schema | Explicit schema in the job: `Time Series (Daily)` as a map (date -> struct of strings), then explode. Prices are cast from string to DECIMAL with no float step. The `processed` table is registered with a `CREATE EXTERNAL TABLE` DDL in `sql/`. No Glue Crawler: it would turn every date into a column, and its result is not guaranteed to be the same on every run. Confirmed in the Databricks spike: schema inference listed every date as a separate field. |
| D3 | Deduplication | The latest `snapshot_date` wins, with `ROW_NUMBER() OVER (PARTITION BY ticker, trade_date ORDER BY snapshot_date DESC) = 1`. `dropDuplicates` is not allowed (non-deterministic). Ties are impossible: one raw file per ticker per day (project 3 key, versioning off). Reason for "latest wins": a run during US trading hours stores a partial day; the next snapshot has the final values. |
| D4 | Rebuild strategy | Full rebuild on every run, plain overwrite (A1). Known risk: an S3 overwrite is not atomic. Mitigation: data quality checks run before the write. Move to incremental processing with a table format (Iceberg/Delta MERGE) if run time or cost grows, or if other readers depend on the table. |
| D5 | Partitioning | No partitioning, one Parquet file (`coalesce`). Reason: the data is a few MB, far below the recommended file size; Spark's default 200 shuffle partitions would create many tiny files. Move to `year` partitions with partition projection at several hundred MB. |
| D6 | Data quality | Strict: any failed check raises, nothing is written. Checks: empty input and empty result; key not NULL and unique; all seven columns not NULL (also detects API schema changes); `low <= high`, `low <= open/close <= high`, `low > 0`, `volume >= 0`; `trade_date <= snapshot_date`. Error message: rule, row count, examples. Success log: files read, rows written, tickers, date range. |
| D7 | Storage and access | Data bucket (`raw/`, `processed/daily_prices/`) plus a separate assets bucket (`scripts/`, `temp/`). Athena managed query results in a dedicated workgroup with a per-query scan limit. Identities: collector user (unchanged), Glue job role (new), developer = the existing Builder ID sign-in (new AWS experience; console IAM users cannot be created). `iam:PassRole` only for the Glue role. Open: confirm that the IAM role can be created in the project account. |
| D8 | Development and testing | Pure functions (parse, deduplicate, validate) tested locally with pytest on in-memory DataFrames built from the project 2 fixture. I/O and Glue-specific code tested by real Glue runs. Local environment: Windows, Python 3.13, PySpark 4.1, JDK 17. Glue 6.0 (Spark 4.1, Python 3.13; about 30% cheaper per DPU-hour than 5.1). Spark 4 ANSI mode turns invalid casts into errors. |
| D9 | Deployment and runs | Job created in the console, settings documented: Glue 6.0, G.1X x 2, timeout 10 min (default would be 480 min), max concurrency 1, retries 0, bookmarks off, Flex execution, parameters `--raw_path` and `--processed_path`. Script upload and job start with the AWS CLI (`aws s3 cp`, `aws glue start-job-run`) using `aws login` and `AWS_CA_BUNDLE`. One script file; `awsglue` imported only in `main()`. |
| D10 | Databricks | Databricks branch as the last stage of project 4, after the Glue part works: Unity Catalog external location (manual setup, read-only, `raw/` prefix), same pure functions, Delta output, Glue vs Databricks comparison in the README. Fallback: upload to a Volume. DataFrame API only (serverless Databricks runs on Spark Connect). |
| D11 | Input data | 5 tickers (IBM, MSFT, AAPL, JPM, XOM), weekdays 9:00 from the Windows Task Scheduler through `scripts/run_daily_collection.ps1` (collector + verifier per ticker, log file in `logs/`, exit code 0/1). |
| D12 | Repository | Same repository and local folder; project 4 is closed with a `v0.4-...` tag. GitHub repository renamed to `financial-data-platform` (the repository grows into the financial data platform of project 9; GitHub redirects the old URL). README title, intro and clone URL are updated when project 4 is closed. |

## Databricks spike (2026-10-28)

| Test | Result |
|---|---|
| Unity Catalog | Free Edition offers "Create a new external location" (automatic and manual). End-to-end validation not tested yet. |
| Network | A notebook reached S3 through a presigned URL: HTTP 200. |
| Spark | Raw JSON read from a Volume; schema inference turned every date into a field, all values strings. DataFrame type is Spark Connect. |

Conclusion: the forum claim that Free Edition cannot read S3 is about key-based access on serverless (`_jsc` is not available on Spark Connect), not about the network.

## Changes to earlier projects

### Project 2: collector exit code (2026-10-28)

- **Problem:** the first scheduled run reported success although nothing was collected. Alpha Vantage rejected all five calls (daily limit), the collector exited with 0 (a planned stop in the project 2 design), and the verifier accepted the object uploaded by an earlier run on the same day.
- **Change:** `collect()` returns `True` if fresh data was written to PostgreSQL and `False` if the daily limit was reached or the API returned no price data. `main()` exits with 0 or 1 accordingly. The S3 upload does not change the result; the verifier checks it.
- **Tests:** three collect tests also check the return value (renamed accordingly); a new parametrized test checks the exit code of `main()`. 38 tests pass. Mutation check: forcing `True`, forcing `False` and a constant `sys.exit(0)` are each caught.
- **Design doc:** project 2 decision "daily limit reached = planned stop, exit code 0" is replaced by "no fresh data = exit code 1".

### Project 2: scheduled collection (2026-10-28)

- `scripts/run_daily_collection.ps1` runs collector + verifier for each ticker; `logs/` added to `.gitignore`.
- Success needs both exit codes to be 0. The verifier alone cannot tell a fresh object from one uploaded earlier the same day.

### Collection wrapper: catch-up runs and IP logging (2026-10-29)

- **Finding:** our `api_calls` table shows 10 calls on 2026-10-28 (5 successful) and 5 on 2026-10-29 (0 successful). Alpha Vantage refused calls with its "25 requests per day" message far below 25 calls from us. The same task succeeded at 15:41 the same day, and a browser call from the company network worked. The scheduler and the code are not the cause; the rejection is intermittent and decided by Alpha Vantage.
- **Hypothesis (unconfirmed):** Alpha Vantage also limits per public IP, and the company network shares its IP(s). Users of shared hosting report the same pattern; their provider attributed it to IP-based limits. Alpha Vantage does not document this.
- **Change:** the wrapper first checks with the verifier whether today's object exists and skips the ticker without an API call if it does. Repeated runs on the same day only retry missing tickers. Additional triggers at 11:00 and 13:30, before the New York open, so catch-up runs still return closed days.
- **Diagnostics:** each run logs its public IP (checkip.amazonaws.com, same route as the collector), so rejections can be matched to IP addresses.

### Root cause of the Alpha Vantage rejections (2026-10-30)

- **Evidence:** all scheduled runs on 2026-10-30 (9:00, 11:00) were rejected. The IP lookup in the wrapper returned the laptop's internal address (172.16.x.x), and `httpbin.org/ip` showed two addresses: the internal one and the company's public IP. The company proxy therefore forwards the internal client IP (`X-Forwarded-For`). In the same minute, an Alpha Vantage call from the laptop browser was rejected and the same call from a phone on mobile data returned data.
- **Conclusion:** the rejections are bound to the company network, not to the API key or our code. Most likely mechanism (not confirmed by Alpha Vantage): the provider counts requests per forwarded client IP, and a private address such as 172.16.x.x is shared by many networks worldwide; or it counts per public IP shared within the company. Either way the fix is outside the code.
- **Options:** (1) the proxy stops forwarding internal IPs to external sites (also a privacy best practice); (2) run the collection from another network; (3) another data source.
- **Impact on project 4:** none for development. The raw layer already has two complete snapshots for all five tickers (2026-10-28 partial day, 2026-10-29) plus earlier IBM snapshots, enough for parsing, deduplication and the partial-day test.

## Observations to follow up

- **API key in the log file.** The Alpha Vantage error message contains the API key and is logged. `logs/` is git-ignored. Consider masking it in the collector.
- **Rate limits.** Alpha Vantage free tier: 25 requests per day and 5 per minute (third-party source, macroption.com). The script pauses 15 s between tickers. Our own counter (UTC day, `api_calls` table) said there was quota left when Alpha Vantage refused; the provider's day boundary or counting may differ.
- **Partial day captured.** The run on 2026-10-28 at 15:12 (Budapest) was during US trading hours, so that snapshot holds a partial 2026-10-28 bar. The next morning run has the final values: a real test case for D3.
- **Scheduler did not start on 2026-10-29.** The condition "start only if the computer is on AC power" was still enabled and the laptop ran on battery at 9:00. The "run as soon as possible after a missed start" setting does not help here, because the start was not missed but blocked by a condition. Fix: condition disabled.
- **Alpha Vantage refused the first call of 2026-10-29** (manual run at 12:11) with the daily-limit message, although only about 10 calls were made the day before. The fixed exit code worked: all five tickers were reported as FAILED. Hypothesis, not confirmed: the limit is also counted per public IP, and the company network shares one IP. Test: one collector run from another network.
