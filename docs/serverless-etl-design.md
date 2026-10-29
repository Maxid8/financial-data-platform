# Project 4 — Serverless ETL with AWS Glue and Athena
## Design Doc

**Status:** design approved, implementation not started
**Builds on:** project 3, the S3 raw layer ([`data-lake-design.md`](data-lake-design.md))
**Working notes:** [`p4-decision-log.md`](p4-decision-log.md)
**Last updated:** 2026-10-29

---

## 1. Problem and Goal

Project 3 stores every API response unchanged in the S3 `raw/` layer. That layer is not fit for analysis:

- **Overlap.** Each file is a snapshot of about 100 trading days, so the same `(ticker, trade_date)` appears in about 100 files.
- **Awkward structure.** The dates are JSON keys, not values. A schema inferred from the files has one field per date and changes every day.
- **No types.** Prices and volume are strings.

This project builds the `processed/` layer: one typed, de-duplicated row per ticker and trading day, stored as Parquet and queryable with SQL from Athena. Three goals:

- **Turn the raw layer into an analytical table** with a serverless Spark job (AWS Glue).
- **Query it in place** with Athena, and measure what Parquet saves compared with JSON.
- **Keep the logic portable.** The same transformation code runs locally in tests, in Glue, and in a Databricks stage at the end of the project.

## 2. Scope

**In scope:**
- A Glue Spark job: raw JSON -> de-duplicated, typed Parquet in `processed/daily_prices/`
- Data quality checks inside the job, before the write
- An Athena table (DDL), a workgroup with a scan limit, and business queries
- Local unit tests for the transformation logic
- A Databricks stage: the same functions reading the raw layer through Unity Catalog, writing a Delta table

**Out of scope (later projects):**
- Scheduling the job — project 6 (Airflow)
- A dimensional model and BI — project 5 (Redshift, Power BI)
- Infrastructure as code — project 8 (Terraform); resources here are created in the console
- Incremental processing and table formats in the Glue path — revisited when the volume requires it (section 5)

## 3. Architecture

```
Windows Task Scheduler (weekdays 9:00)
  -> scripts/run_daily_collection.ps1  (5 tickers: collector + verifier)
       -> S3 raw/year=YYYY/month=MM/day=DD/TICKER.json
                    |
                    v
          Glue job (Spark 4.1, Glue 6.0, Flex)
            read -> parse -> deduplicate -> validate -> write
                    |
                    v
          S3 processed/daily_prices/  (one Parquet file)
                    |
                    v
          Glue Data Catalog table (created by DDL) -> Athena (workgroup, managed results)

Stage 2: Databricks Free Edition
  Unity Catalog external location (read-only, raw/) -> same functions -> Delta table
```

## 4. Components in Detail

### 4.1 Input

`s3://<data-bucket>/raw/year=YYYY/month=MM/day=DD/TICKER.json`: the unchanged `TIME_SERIES_DAILY` (compact) response, one file per ticker per run day (project 3 contract). From 2026-10-28 five tickers are collected on weekdays: IBM, MSFT, AAPL, JPM, XOM.

### 4.2 Output table `daily_prices`

One row per ticker and trading day. Key: `(ticker, trade_date)`.

| Column | Type | Source |
|---|---|---|
| `ticker` | STRING | `Meta Data` -> `2. Symbol` |
| `trade_date` | DATE | the key of `Time Series (Daily)` |
| `open`, `high`, `low`, `close` | DECIMAL(12,4) | `1. open` ... `4. close` |
| `volume` | BIGINT | `5. volume` |
| `snapshot_date` | DATE | `year=/month=/day=` of the raw key |

- **DECIMAL, not DOUBLE.** Same precision as `NUMERIC(12,4)` in the project 2 PostgreSQL table, so both layers hold bit-identical values. Calculations (returns, volatility) cast to DOUBLE in the query.
- **`snapshot_date` gives lineage.** With `ticker` it rebuilds the raw key through `build_raw_key`; no file-name column is needed.
- **No `processed_at`.** With a full rebuild every row would carry the same value. The run time is in the job log.
- **The key is not enforced by Athena.** The job guarantees it and checks it (4.4).

Location: `s3://<data-bucket>/processed/daily_prices/`. The table has its own folder because Athena reads every file under a table's `LOCATION`.

### 4.3 The Glue job

One script file. The transformation is split into pure functions that only take and return DataFrames, so they run in local tests, in Glue and in Databricks without changes. Only `main()` contains Glue-specific code (`awsglue` is imported there).

| Function | Kind | Responsibility |
|---|---|---|
| `read_raw(spark, path)` | I/O | read the raw layer with an explicit schema; Spark adds `year`, `month`, `day` from the Hive-style folders |
| `parse_snapshots(df)` | pure | explode the date map into rows, cast types, build `snapshot_date` |
| `deduplicate(df)` | pure | keep the latest snapshot per `(ticker, trade_date)` |
| `validate(df)` | pure | apply the checks of 4.4; raise on failure |
| `write_processed(df, path)` | I/O | `coalesce(1)`, overwrite as Parquet |
| `main()` | Glue | read job parameters, call the steps, log the run summary |

**Explicit schema, no inference.** `Time Series (Daily)` is read as a map: key = date string, value = struct of five strings. Inference would create one field per date; the Databricks spike confirmed this (section 9). Prices are cast from string directly to DECIMAL, with no float step.

**Deduplication.** `ROW_NUMBER() OVER (PARTITION BY ticker, trade_date ORDER BY snapshot_date DESC) = 1`. The latest snapshot wins because a run during US trading hours stores a partial day, and a later snapshot has the final values. Ties are impossible: the raw key allows one file per ticker per day. `dropDuplicates` is not used, because it keeps an arbitrary row.

**DataFrame API only.** No RDDs and no `sparkContext`: serverless Databricks runs on Spark Connect, where these are not available.

### 4.4 Data quality

Strict rule: any failed check raises an exception, nothing is written, and the previous `processed` data stays untouched. The Glue run ends in `FAILED`.

| Check | Why |
|---|---|
| at least one raw file, and at least one row after parsing | a full overwrite with empty input would empty the table |
| `ticker` and `trade_date` not NULL; `(ticker, trade_date)` unique | the key; also tests the deduplication |
| all seven value columns not NULL | a renamed API field becomes NULL silently; this check detects schema changes |
| `low <= high`, `low <= open <= high`, `low <= close <= high` | a daily candle must be consistent |
| `low > 0`, `volume >= 0` | prices are positive, volume is not negative |
| `trade_date <= snapshot_date` | a snapshot cannot contain a future trading day |

The error message names the rule, the number of failing rows and a few examples (ticker, trade_date). A successful run logs the number of raw files, rows written, tickers and the date range.

Spark 4 runs in ANSI mode by default, so an invalid string-to-DECIMAL cast raises instead of returning NULL. Whether Glue keeps this default is checked on the first run.

### 4.5 Storage layout

| Bucket | Prefix | Content |
|---|---|---|
| data bucket (existing) | `raw/` | project 3 raw layer |
| | `processed/daily_prices/` | the Parquet output |
| assets bucket (new) | `scripts/` | the Glue job script |
| | `temp/` | Glue temporary directory; lifecycle rule deletes objects after a few days |

Data and operational files are separated: the data bucket keeps its project 3 protection, and the assets bucket can have its own lifecycle rules.

Athena uses **managed query results**: Athena stores results for 24 hours at no extra cost and deletes them, so no results bucket or prefix is needed.

### 4.6 Access

| Identity | Type | Permissions |
|---|---|---|
| `daily-prices-collector` | IAM user (project 3) | unchanged: `raw/` only |
| Glue job role | IAM role, trusted by `glue.amazonaws.com` | read `raw/`; read, write and delete `processed/daily_prices/`; read `scripts/`; read and write `temp/`; write CloudWatch Logs under `/aws-glue/` |
| developer | Builder ID sign-in (new AWS experience) | console and CLI work; `iam:PassRole` limited to the Glue role |

- The job does not touch the Data Catalog: the table is created once with DDL. The role therefore needs no Glue Catalog permissions.
- Delete on `processed/` is required, because an overwrite removes the previous files.
- Local CLI access uses `aws login` (AWS CLI 2.32+): temporary credentials from the console session, no access keys. Behind the corporate proxy the CLI needs `AWS_CA_BUNDLE`, as boto3 does.
- The new AWS experience does not allow IAM users with console access. Creating the Glue role in the project account is the first step of the implementation, because it is not yet confirmed.

Draft permissions policy for the Glue role (placeholders, validated on the first run):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListDataPrefixes",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::YOUR_DATA_BUCKET",
      "Condition": { "StringLike": { "s3:prefix": ["raw/*", "processed/daily_prices/*"] } }
    },
    {
      "Sid": "ReadRaw",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::YOUR_DATA_BUCKET/raw/*"
    },
    {
      "Sid": "WriteProcessed",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::YOUR_DATA_BUCKET/processed/daily_prices/*"
    },
    {
      "Sid": "ListAssets",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::YOUR_ASSETS_BUCKET",
      "Condition": { "StringLike": { "s3:prefix": ["scripts/*", "temp/*"] } }
    },
    {
      "Sid": "ReadScripts",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::YOUR_ASSETS_BUCKET/scripts/*"
    },
    {
      "Sid": "UseTemp",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::YOUR_ASSETS_BUCKET/temp/*"
    },
    {
      "Sid": "WriteLogs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:*:*:log-group:/aws-glue/*"
    }
  ]
}
```

### 4.7 Job configuration

Created in the console; these settings are the source of truth until project 8 moves them to Terraform.

| Setting | Value | Why |
|---|---|---|
| Glue version | 6.0 (Spark 4.1, Python 3.13) | newest runtime, about 30% cheaper per DPU-hour than 5.1, no migration burden for a new job |
| Worker type and count | G.1X x 2 | the smallest configuration is more than enough |
| Execution class | Flex | cheaper; a delayed start is acceptable |
| Timeout | 10 minutes | the default for Glue 5.0+ is 480 minutes; a stuck job would bill for hours |
| Max concurrency | 1 | two parallel full overwrites would corrupt each other |
| Retries | 0 | a failure is investigated, not repeated blindly |
| Job bookmarks | disabled | full rebuild, no state |
| Parameters | `--raw_path`, `--processed_path` | no bucket names in the code |
| Tags | `Project = daily-prices-datalake` | cost allocation, as in project 3 |

### 4.8 Athena

- Database `financial_data` (name proposed), table `daily_prices`, created with `CREATE EXTERNAL TABLE ... STORED AS PARQUET LOCATION ...` in `sql/athena_ddl.sql`.
- A dedicated workgroup with managed query results and a per-query scan limit.
- `sql/athena_queries.sql`: business questions, for example daily return, 20-day moving average, volatility per ticker, largest drawdown.

### 4.9 Development and deployment

- **Local tests.** `parse_snapshots`, `deduplicate` and `validate` are tested with pytest on in-memory DataFrames built from the project 2 fixture. Tests do not write files, so Spark on Windows needs no extra Hadoop binaries. Local environment: Python 3.13 in a separate virtual environment, PySpark 4.1.x, JDK 17.
- **Upload and run.** `aws s3 cp` uploads the script, `aws glue start-job-run` starts the job. The commands are in the README; project 6 calls the same API.
- **Integration.** I/O and Glue-specific code are tested by real Glue runs.

### 4.10 Databricks stage

After the Glue path works:

1. IAM role for Databricks (manual setup, read-only, `raw/` prefix), storage credential and external location in Unity Catalog.
2. A notebook imports the same pure functions, reads the raw layer through the external location and writes a Delta table.
3. The README compares Glue and Databricks: setup, cost, development loop, table format.

Fallback if the external location cannot be validated in Free Edition: raw files uploaded to a Volume.

## 5. Key Design Decisions

| Decision | Alternatives | Rationale |
|---|---|---|
| One table with a `ticker` column | one table per ticker | new tickers need no change; comparisons are a `GROUP BY`; one schema |
| DECIMAL(12,4) | DOUBLE | exact values, identical to PostgreSQL; DOUBLE only in calculations |
| `snapshot_date` column | `last_updated`, `processed_at` | precise meaning, lineage through the raw key; `processed_at` would be constant |
| Explicit map schema in the job | Glue Crawler | the crawler would create one column per date, and its result is not guaranteed to be stable |
| DDL for the processed table | crawler, catalog update from the job | the schema stays exactly as designed, versioned in git |
| Latest snapshot wins, `ROW_NUMBER` | earliest wins; `dropDuplicates` | partial days are replaced by final values; deterministic |
| Full rebuild, plain overwrite | incremental with bookmarks; write-then-swap | a few MB of data; simple and self-healing. Accepted risk: the overwrite is not atomic. Mitigated by validating before the write |
| No partitioning, one file | `ticker`, `year`, both | far below the recommended file size; no partition registration |
| Strict data quality | quarantine, mixed | small, financial data; a failed run leaves the old table intact |
| Separate assets bucket | prefixes in the data bucket | data and operational files apart; own lifecycle rules |
| Athena managed results | results prefix in S3 | no bucket, no cleanup, no extra permissions |
| Builder ID + `aws login` | IAM user with access keys | temporary credentials; console IAM users are not available in the new AWS experience |
| Pure functions, local pytest | developing in Glue | fast feedback, no cost per attempt |
| Glue 6.0, Flex | 5.1, Standard | cheaper, newest runtime; delayed start accepted |
| Console job + CLI deploy | console only; IaC now | console for learning, CLI for repeatable runs (same API as Airflow); IaC in project 8 |
| Databricks as the last stage | skip; do it first | Glue stays the focus; the spike showed a viable path |
| No Glue Crawler (deviation from the original plan) | crawler on `raw/` | see above; documented as a deliberate deviation |

**When to change course:** move to incremental processing with a table format (Iceberg or Delta, `MERGE`) and to `year` partitions with partition projection when the table reaches several hundred MB, when the run time or cost grows noticeably, or when other readers depend on the table during a run.

## 6. The Processed Layer as a Contract

- The schema in 4.2 is a promise to project 5 (Redshift) and to every query. It changes only deliberately, with the DDL, the job and the tests in one commit.
- The key `(ticker, trade_date)` is unique; the job enforces it.
- `processed/daily_prices/` contains only this table's files.

## 7. Cost and Security

- Glue: a minimal Flex run on 2 workers costs cents; the 10-minute timeout caps a stuck run.
- Athena: billed per data scanned (10 MB minimum per query); Parquet and the workgroup scan limit keep it low.
- The account's spend limit and alerts from project 3 stay in place (the new AWS experience).
- No access keys for development. The Glue role has no permission outside its prefixes and no Data Catalog permission.
- The API key appears in the collector log when Alpha Vantage returns an error. `logs/` is git-ignored; masking is a follow-up (section 10).

## 8. Testing

- **Unit tests (local):** parsing (types, explode, `snapshot_date`), deduplication (latest wins, partial day replaced), every data quality rule with a passing and a failing case.
- **Integration (Glue):** first run checks the IAM policy, ANSI behaviour, the output file count and the log summary.
- **End to end:** Athena row counts per ticker match the expected trading days; a value from `processed` equals the same value in PostgreSQL.
- **Measurement:** stored bytes of `raw/` vs `processed/`, and Athena "data scanned" per query. Open: whether a temporary Athena table over the raw JSON is created for a like-for-like query comparison.
- **Real partial-day case:** the 2026-10-28 snapshot was taken during US trading hours; the following snapshot must replace that day's values.

## 9. Databricks Spike (2026-10-28)

| Test | Result |
|---|---|
| Unity Catalog | Free Edition offers creating an external location (automatic and manual setup). End-to-end validation not tested yet. |
| Network | A notebook reached S3 through a presigned URL (HTTP 200). |
| Spark | Raw JSON read from a Volume. Inferred schema: one field per date, all values strings. DataFrames are Spark Connect DataFrames. |

The community claim that Free Edition cannot read S3 concerns key-based access on serverless compute, which Spark Connect does not support, not the network.

## 10. Risks and Open Questions

- *Open:* can the Glue IAM role be created in the new AWS experience project account? First implementation step.
- *Open:* Alpha Vantage rejected the first call of 2026-10-29 with its daily-limit message. Hypothesis: limits also apply per public IP, and the company network shares one. Test from another network. If confirmed, the input for this project needs another collection setup.
- *Open:* does Glue 6.0 keep Spark's ANSI mode default?
- *Open:* Glue 6.0 availability and price in `eu-north-1` are confirmed when the job is created.
- *Accepted:* non-atomic overwrite (section 5).
- *Accepted:* the scheduled collection depends on the laptop being on.
- *Follow-up:* mask the API key in collector logs.

## 11. Implementation Plan

1. Branch `serverless-etl`; local Spark environment (Python 3.13, PySpark 4.1, JDK 17).
2. Pure functions and their tests: parse, deduplicate, validate.
3. AWS resources: Glue role (first), assets bucket, `iam:PassRole`, Athena workgroup.
4. Job script `main()`, upload, first Glue run, fixes.
5. Athena DDL, business queries, measurement.
6. Databricks stage.
7. Documentation (this doc, README, project 2 doc update), merge, tag `v0.4-serverless-etl`.

## 12. Changes to Earlier Projects

- **Project 2, collector exit code (2026-10-28):** `collect()` returns whether fresh data was written; `main()` exits with 1 when nothing was collected. Found when the first scheduled run reported success without new data. Replaces the project 2 decision "daily limit reached = planned stop, exit code 0".
- **Project 2, scheduled collection (2026-10-28):** `scripts/run_daily_collection.ps1` and the Windows Task Scheduler task; details in [`p4-decision-log.md`](p4-decision-log.md).
- **Repository:** renamed on GitHub to `financial-data-platform` (2026-10-29).

## 13. Lessons Learned

*To be written at the end of the project.*
