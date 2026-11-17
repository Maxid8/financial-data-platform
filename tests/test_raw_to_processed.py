"""Unit tests for raw_to_processed.py (project 4 Glue job).

They need PySpark and Java, so they run in the Spark environment (WSL):
    pytest tests/test_raw_to_processed.py

In the collector environment (.venv, no PySpark) this module is skipped.
The raw fixtures are two real IBM snapshots (2026-10-28 and 2026-10-29):
100 trading days each, 99 of them overlapping, 101 distinct days in total.
"""

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from raw_to_processed import (  # noqa: E402
    OUTPUT_COLUMNS,
    RAW_SCHEMA,
    DataQualityError,
    deduplicate,
    parse_snapshots,
    read_raw,
    validate,
    write_processed,
)

RAW_FIXTURES = str(Path(__file__).parent / "fixtures" / "raw")

PROCESSED_SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("trade_date", DateType()),
    StructField("open", DecimalType(12, 4)),
    StructField("high", DecimalType(12, 4)),
    StructField("low", DecimalType(12, 4)),
    StructField("close", DecimalType(12, 4)),
    StructField("volume", LongType()),
    StructField("snapshot_date", DateType()),
])

# RAW_SCHEMA plus the columns partition discovery adds.
RAW_WITH_PARTITIONS = StructType(RAW_SCHEMA.fields + [
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("day", IntegerType()),
])


@pytest.fixture(scope="module")
def spark():
    """One local Spark session for all tests in this module (starting one takes seconds)."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("raw_to_processed-tests")
        .config("spark.sql.ansi.enabled", "true")      # same as the Glue job
        .config("spark.sql.shuffle.partitions", "1")   # tiny data: no need for 200
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def price_row(
    trade_date=dt.date(2026, 10, 27),
    open_="250.0000",
    high="255.0000",
    low="245.0000",
    close="252.0000",
    volume=1_000_000,
    snapshot_date=dt.date(2026, 10, 28),
    ticker="IBM",
):
    """A valid processed row; each test changes only what it is about."""
    return (
        ticker,
        trade_date,
        None if open_ is None else Decimal(open_),
        None if high is None else Decimal(high),
        None if low is None else Decimal(low),
        None if close is None else Decimal(close),
        volume,
        snapshot_date,
    )


def processed_df(spark, *rows):
    return spark.createDataFrame(list(rows), PROCESSED_SCHEMA)


# --- read_raw -------------------------------------------------------------------

def test_read_raw_returns_one_row_per_file_with_partition_columns(spark):
    raw = read_raw(spark, RAW_FIXTURES)

    assert raw.count() == 2
    assert raw.columns == ["Meta Data", "Time Series (Daily)", "year", "month", "day"]
    assert raw.schema["Time Series (Daily)"].dataType.typeName() == "map"
    assert sorted(row["day"] for row in raw.select("day").collect()) == [28, 29]


# --- parse_snapshots ------------------------------------------------------------

def test_parse_snapshots_returns_one_row_per_day_and_snapshot_with_output_types(spark):
    parsed = parse_snapshots(read_raw(spark, RAW_FIXTURES))

    assert parsed.count() == 200  # 2 snapshots x 100 days; duplicates are kept here
    # Names, order and types; nullability is not part of the contract.
    assert parsed.dtypes == [(f.name, f.dataType.simpleString()) for f in PROCESSED_SCHEMA.fields]


def test_parse_snapshots_maps_fields_and_snapshot_date(spark):
    parsed = parse_snapshots(read_raw(spark, RAW_FIXTURES))

    row = parsed.filter(
        (parsed.trade_date == dt.date(2026, 10, 28)) & (parsed.snapshot_date == dt.date(2026, 10, 29))
    ).collect()

    # Values from tests/fixtures/raw/year=2026/month=10/day=29/IBM.json
    assert [tuple(r) for r in row] == [(
        "IBM",
        dt.date(2026, 10, 28),
        Decimal("248.4900"),
        Decimal("253.8500"),
        Decimal("247.6700"),
        Decimal("253.2200"),
        3706461,
        dt.date(2026, 10, 29),
    )]


def test_parse_snapshots_raises_on_a_price_that_is_not_a_number(spark):
    raw = spark.createDataFrame(
        [(
            {"2. Symbol": "IBM"},
            {"2026-10-28": {"1. open": "abc", "2. high": "1", "3. low": "1", "4. close": "1", "5. volume": "1"}},
            2026, 10, 29,
        )],
        RAW_WITH_PARTITIONS,
    )

    # ANSI mode: an invalid cast is an error, not a silent NULL.
    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
        parse_snapshots(raw).collect()


# --- deduplicate ------------------------------------------------------------------

def test_deduplicate_keeps_one_row_per_ticker_and_day(spark):
    deduped = deduplicate(parse_snapshots(read_raw(spark, RAW_FIXTURES)))

    assert deduped.count() == 101  # 100 + 100 days, 99 of them overlapping
    assert deduped.select("ticker", "trade_date").distinct().count() == 101


def test_deduplicate_keeps_the_latest_snapshot_when_values_differ(spark):
    older = price_row(close="252.0000", snapshot_date=dt.date(2026, 10, 28))
    newer = price_row(close="253.0000", snapshot_date=dt.date(2026, 10, 29))

    deduped = deduplicate(processed_df(spark, older, newer))

    assert [tuple(r) for r in deduped.collect()] == [newer]


# --- validate -----------------------------------------------------------------------

def test_validate_passes_on_the_fixture_data(spark):
    validate(deduplicate(parse_snapshots(read_raw(spark, RAW_FIXTURES))))


def test_validate_raises_on_empty_input(spark):
    with pytest.raises(DataQualityError, match="No rows to write"):
        validate(processed_df(spark))


def test_validate_raises_on_null_value_and_stops_there(spark):
    # low is NULL; the other rules would not see this row, so validate stops at the NULL rule.
    row = price_row(low=None)

    with pytest.raises(DataQualityError, match="Rule 'no NULL values' failed for 1 rows, e.g. IBM 2026-10-27") as exc_info:
        validate(processed_df(spark, row))

    assert "low <= high" not in str(exc_info.value)


def test_validate_raises_on_duplicate_keys(spark):
    row = price_row()

    with pytest.raises(DataQualityError, match=r"Rule 'unique \(ticker, trade_date\)' failed for 1 rows"):
        validate(processed_df(spark, row, row))


@pytest.mark.parametrize(
    "row, rule",
    [
        (price_row(low="256.0000"), "low <= high"),
        (price_row(open_="260.0000"), "low <= open <= high"),
        (price_row(close="240.0000"), "low <= close <= high"),
        (price_row(open_="0.0000", low="0.0000", close="1.0000"), "low > 0"),
        (price_row(volume=-1), "volume >= 0"),
        (price_row(trade_date=dt.date(2026, 10, 29), snapshot_date=dt.date(2026, 10, 28)), "trade_date <= snapshot_date"),
    ],
    ids=["low_above_high", "open_outside_range", "close_outside_range", "zero_price", "negative_volume", "future_trade_date"],
)
def test_validate_raises_on_business_rule_violation(spark, row, rule):
    with pytest.raises(DataQualityError, match=f"Rule '{rule}' failed for 1 rows"):
        validate(processed_df(spark, row))


def test_validate_reports_all_failed_rules_at_once(spark):
    # Breaks two independent rules.
    row = price_row(volume=-1, trade_date=dt.date(2026, 10, 29), snapshot_date=dt.date(2026, 10, 28))

    with pytest.raises(DataQualityError) as exc_info:
        validate(processed_df(spark, row))

    message = str(exc_info.value)
    assert "Rule 'volume >= 0'" in message
    assert "Rule 'trade_date <= snapshot_date'" in message


# --- write_processed ------------------------------------------------------------------

def test_write_processed_writes_one_parquet_file_that_reads_back_unchanged(spark, tmp_path):
    rows = [price_row(), price_row(trade_date=dt.date(2026, 10, 28), snapshot_date=dt.date(2026, 10, 29))]
    output = tmp_path / "daily_prices"

    # Two partitions in, so a missing coalesce(1) would show up as two files.
    write_processed(processed_df(spark, *rows).repartition(2), str(output))

    assert len(list(output.glob("*.parquet"))) == 1
    read_back = spark.read.parquet(str(output))
    assert read_back.schema == PROCESSED_SCHEMA
    assert sorted(tuple(r) for r in read_back.collect()) == sorted(rows)


def test_write_processed_overwrites_the_previous_run(spark, tmp_path):
    output = str(tmp_path / "daily_prices")

    write_processed(processed_df(spark, price_row(), price_row(trade_date=dt.date(2026, 10, 26))), output)
    write_processed(processed_df(spark, price_row()), output)

    assert spark.read.parquet(output).count() == 1


def test_output_columns_match_the_processed_schema():
    assert OUTPUT_COLUMNS == PROCESSED_SCHEMA.fieldNames()
