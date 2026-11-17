"""Glue job (project 4): S3 raw layer -> processed daily_prices table (Parquet).

Reads every raw Alpha Vantage snapshot (one JSON file per ticker per run day),
turns the date-keyed map into one row per ticker and trading day, keeps the
latest snapshot of each day, checks the data quality rules, and overwrites
processed/daily_prices/ with a single Parquet file.

Steps (design doc section 4.3):
  read_raw -> parse_snapshots -> deduplicate -> validate -> write_processed

The step functions only use PySpark, so the tests run them locally. Only main()
contains Glue-specific code: the awsglue import happens there.

Glue job parameters:
    --raw_path        s3://<data-bucket>/raw/
    --processed_path  s3://<data-bucket>/processed/daily_prices/
"""

import logging
import sys
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, StructField, StructType

# Explicit name instead of __name__: Glue runs this file as a script,
# so __name__ would be "__main__", which would say nothing in the log.
logger = logging.getLogger("raw_to_processed")

# --- Raw layer: field names exactly as in the Alpha Vantage response --------

META_DATA = "Meta Data"
SYMBOL = "2. Symbol"
TIME_SERIES = "Time Series (Daily)"

# Output column -> field name inside one day of "Time Series (Daily)".
PRICE_FIELDS = {
    "open": "1. open",
    "high": "2. high",
    "low": "3. low",
    "close": "4. close",
}
VOLUME_FIELD = "5. volume"

# Only the fields listed here are read; everything else in the file is ignored.
# All values are strings in the response, and stay strings until
# parse_snapshots casts them (string -> DECIMAL, no float step).
RAW_SCHEMA = StructType([
    StructField(META_DATA, StructType([
        StructField(SYMBOL, StringType()),
    ])),
    StructField(TIME_SERIES, MapType(
        StringType(),  # key: trading day, e.g. "2026-10-28"
        StructType(
            [StructField(field, StringType()) for field in PRICE_FIELDS.values()]
            + [StructField(VOLUME_FIELD, StringType())]
        ),
    )),
])

# --- Processed layer: the daily_prices contract (design doc section 4.2) ----

PRICE_TYPE = "decimal(12,4)"
KEY_COLUMNS = ["ticker", "trade_date"]
OUTPUT_COLUMNS = ["ticker", "trade_date", "open", "high", "low", "close", "volume", "snapshot_date"]

# How many failing rows are quoted in a data quality error message.
EXAMPLE_LIMIT = 3


class DataQualityError(Exception):
    """Raised by validate(): the data must not be written."""


# --- Steps -------------------------------------------------------------------

def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read every raw snapshot under path. One row per file.

    Spark walks the year=/month=/day= folders itself (partition discovery)
    and adds year, month and day as integer columns.
    """
    return (
        spark.read
        .schema(RAW_SCHEMA)
        .option("multiLine", True)  # one JSON object spread over many lines
        .json(path)
    )


def parse_snapshots(raw: DataFrame) -> DataFrame:
    """One row per ticker, trading day and snapshot, with typed columns.

    Keeps duplicates: the same trading day appears once per snapshot that
    contains it. deduplicate() decides which one stays.
    """
    exploded = raw.select(
        F.col(META_DATA).getField(SYMBOL).alias("ticker"),
        # explode on a map gives one row per entry: key and value.
        F.explode(F.col(TIME_SERIES)).alias("trade_day", "prices"),
        "year",
        "month",
        "day",
    )
    price_columns = [
        F.col("prices").getField(field).cast(PRICE_TYPE).alias(name)
        for name, field in PRICE_FIELDS.items()
    ]
    return exploded.select(
        "ticker",
        F.to_date("trade_day", "yyyy-MM-dd").alias("trade_date"),
        *price_columns,
        F.col("prices").getField(VOLUME_FIELD).cast("bigint").alias("volume"),
        F.make_date("year", "month", "day").alias("snapshot_date"),
    )


def deduplicate(parsed: DataFrame) -> DataFrame:
    """Keep one row per (ticker, trade_date): the one from the latest snapshot.

    ROW_NUMBER, not dropDuplicates: dropDuplicates keeps an arbitrary row.
    Ties are impossible, because the raw layer has one file per ticker per day.
    """
    latest_first = Window.partitionBy(*KEY_COLUMNS).orderBy(F.col("snapshot_date").desc())
    return (
        parsed
        .withColumn("row_number", F.row_number().over(latest_first))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


# --- Data quality rules --------------------------------------------------------
# Each finder returns the violating rows; an empty result means the rule passed.

def find_null_values(df: DataFrame) -> DataFrame:
    """Rows with a NULL in any output column.

    A renamed field in the API response is read as NULL, so this rule also
    detects schema changes.
    """
    any_null = reduce(lambda left, right: left | right, [F.col(c).isNull() for c in OUTPUT_COLUMNS])
    return df.filter(any_null)


def find_duplicate_keys(df: DataFrame) -> DataFrame:
    """(ticker, trade_date) pairs that occur more than once."""
    return df.groupBy(*KEY_COLUMNS).count().filter(F.col("count") > 1)


def business_rules() -> dict[str, Column]:
    """Rule name -> condition that every row must satisfy.

    Built in a function, not at module level, because a Column needs an
    active Spark session.
    """
    low, high = F.col("low"), F.col("high")
    return {
        "low <= high": low <= high,
        "low <= open <= high": F.col("open").between(low, high),
        "low <= close <= high": F.col("close").between(low, high),
        "low > 0": low > 0,
        "volume >= 0": F.col("volume") >= 0,
        "trade_date <= snapshot_date": F.col("trade_date") <= F.col("snapshot_date"),
    }


def find_violations(df: DataFrame, condition: Column) -> DataFrame:
    """Rows that do not satisfy condition. Expects no NULLs (checked first)."""
    return df.filter(~condition)


def describe_failure(rule: str, violations: DataFrame) -> str | None:
    """One line for the error message, or None if the rule passed."""
    count = violations.count()
    if count == 0:
        return None
    examples = ", ".join(
        f"{row['ticker']} {row['trade_date']}"
        for row in violations.select(*KEY_COLUMNS).limit(EXAMPLE_LIMIT).collect()
    )
    return f"Rule '{rule}' failed for {count} rows, e.g. {examples}"


def validate(df: DataFrame) -> None:
    """Apply the data quality rules (design doc 4.4). Raise DataQualityError on failure.

    Order matters:
      1. an empty result stops here, a full overwrite would empty the table;
      2. NULLs stop here, because a comparison with NULL is unknown and the
         later rules would silently pass those rows;
      3. all other rules run, and every failure goes into one error message.
    """
    if df.isEmpty():
        raise DataQualityError("No rows to write: the raw layer is empty or nothing could be parsed.")

    null_failure = describe_failure("no NULL values", find_null_values(df))
    if null_failure:
        raise DataQualityError(null_failure)

    failures = [describe_failure("unique (ticker, trade_date)", find_duplicate_keys(df))]
    for rule, condition in business_rules().items():
        failures.append(describe_failure(rule, find_violations(df, condition)))

    failures = [failure for failure in failures if failure]
    if failures:
        raise DataQualityError("Data quality checks failed:\n" + "\n".join(failures))


def write_processed(df: DataFrame, path: str) -> None:
    """Overwrite path with one Parquet file (design doc 4.3, D5).

    coalesce(1): after the window in deduplicate, Spark would otherwise write
    one small file per shuffle partition.
    """
    df.coalesce(1).write.mode("overwrite").parquet(path)


# --- Glue entry point ------------------------------------------------------------

def log_summary(raw_files: int, processed: DataFrame) -> None:
    """Log what the run produced, in one Spark job."""
    stats = processed.agg(
        F.count("*").alias("rows"),
        F.countDistinct("ticker").alias("tickers"),
        F.min("trade_date").alias("first_day"),
        F.max("trade_date").alias("last_day"),
    ).first()
    logger.info(
        "Raw files read: %d. Rows written: %d. Tickers: %d. Trading days: %s to %s.",
        raw_files, stats["rows"], stats["tickers"], stats["first_day"], stats["last_day"],
    )


def main() -> None:
    """Glue entry point: read the job parameters, run the steps, log the result."""
    from awsglue.utils import getResolvedOptions  # only available in Glue

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = getResolvedOptions(sys.argv, ["raw_path", "processed_path"])

    spark = SparkSession.builder.getOrCreate()
    # Invalid casts raise instead of returning NULL (Spark 4 default; set explicitly).
    spark.conf.set("spark.sql.ansi.enabled", "true")

    logger.info("Reading raw layer: %s", args["raw_path"])
    raw = read_raw(spark, args["raw_path"])
    raw_files = raw.count()

    # cache(): validate() and the write both read this result; without it,
    # every action would read and parse the raw layer again.
    processed = deduplicate(parse_snapshots(raw)).cache()

    try:
        validate(processed)
    except DataQualityError:
        logger.error("Data quality checks failed; nothing was written.")
        raise

    write_processed(processed, args["processed_path"])
    log_summary(raw_files, processed)
    logger.info("Processed layer written: %s", args["processed_path"])


if __name__ == "__main__":
    main()
