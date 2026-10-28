"""Daily stock price collector (Alpha Vantage -> PostgreSQL -> S3 raw layer).

One run loads one ticker:
  1. checks the daily API quota (25 calls/day, counted in the api_calls table),
  2. fetches TIME_SERIES_DAILY from Alpha Vantage (or reads a saved fixture),
  3. validates the response by its content,
  4. upserts the prices into daily_prices and commits,
  5. uploads the raw response to the S3 raw layer (best-effort, live mode only).

Usage:
    python src/API_call.py --ticker IBM [--fixture tests/fixtures/daily_series_success.json]
"""

import argparse
import os
import ssl
import sys
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import NamedTuple
import datetime as dt

import psycopg
import truststore
from dotenv import load_dotenv

from s3_writer import upload_raw_to_s3


PROJECT_ROOT = Path(__file__).resolve().parent.parent
REQUIRED_ENV_VARS = ["API_KEY", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME", "S3_BUCKET_NAME"]
DAILY_CALL_LIMIT = 25
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"

# Explicit name instead of __name__: when this file is run as a script,
# __name__ is "__main__", which would say nothing in the log.
logger = logging.getLogger("daily_api_collector")


class ValidationResult(NamedTuple):
    """Outcome of validate_response: either result or error is set, never both."""
    is_valid: bool
    result: dict | None
    error: dict | None


def get_config() -> dict[str, str]:
    """Load .env and return the required settings. Fail fast if any is missing or empty."""
    load_dotenv(PROJECT_ROOT / ".env")
    missing = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing:
        raise RuntimeError(
            f"Missing environment variables: {', '.join(missing)}. "
            "Create an .env file in the root of your project based on .env.example."
        )
    return {var: os.environ[var] for var in REQUIRED_ENV_VARS}


def get_connection(config: dict[str, str]) -> psycopg.Connection:
    """Open a PostgreSQL connection from the loaded settings."""
    return psycopg.connect(
        dbname=config["DB_NAME"],
        user=config["DB_USER"],
        password=config["DB_PASSWORD"],
        host=config["DB_HOST"],
        port=config["DB_PORT"],
    )


def calls_made_today(cur: psycopg.Cursor) -> int:
    """Successful calls so far today, UTC — see design doc 4.2/8 on why UTC."""
    cur.execute(
        "SELECT COUNT(*) FROM api_calls "
        "WHERE success AND called_at::date = (now() AT TIME ZONE 'UTC')::date"
    )
    row = cur.fetchone()
    assert row is not None, "COUNT(*) always returns exactly one row"
    return row[0]


def fetch_daily_prices(ticker: str, api_key: str) -> dict:
    """Live call to Alpha Vantage. Network errors retry with backoff (design doc 4.4);
    a response that parses fine but signals quota/format trouble is NOT a network
    error and is handled by validate_response instead."""
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "apikey": api_key,
        "outputsize": "compact",
    }
    url = f"{ALPHA_VANTAGE_URL}?{urllib.parse.urlencode(params)}"

    # Verify TLS certificates against the OS's native trust store (Windows:
    # SChannel) instead of Python's bundled one. Needed on networks with
    # TLS-inspecting proxies whose certificates the OS already trusts.
    # Verification still happens, just against the OS's trust anchors.
    # Scoped to this call on purpose: truststore.inject_into_ssl() would
    # replace ssl.SSLContext globally and break boto3 (RecursionError).
    ssl_context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=10, context=ssl_context) as response:
                return json.loads(response.read().decode())
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            wait = 2**attempt
            logger.warning(
                "Network error calling Alpha Vantage (attempt %d/3): %s. Retrying in %ds.",
                attempt + 1, exc, wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"Alpha Vantage request failed after retries: {last_error}")


def validate_response(response: dict) -> ValidationResult:
    """Success is judged by CONTENT (expected key present), not by the response
    merely being valid JSON — Alpha Vantage returns 200 + valid JSON even on a
    quota breach, just with a 'Note'/'Information' key instead."""
    if "Time Series (Daily)" in response:
        return ValidationResult(is_valid=True, result=response, error=None)
    return ValidationResult(is_valid=False, result=None, error=response)


def record_call(cur: psycopg.Cursor, ticker: str, is_valid: bool, error: dict | None) -> None:
    """Log one API call in api_calls; the daily quota is counted from this table."""
    error_reason = None
    if error is not None:
        error_reason = error.get("Note") or error.get("Information") or json.dumps(error)[:500]
    cur.execute(
        "INSERT INTO api_calls (ticker, success, error_reason) VALUES (%s, %s, %s)",
        (ticker, is_valid, error_reason),
    )


def upsert_daily_prices(cur: psycopg.Cursor, ticker: str, price_data: dict) -> int:
    """Insert or update one row per trading day; return the number of rows sent.

    ON CONFLICT on (ticker, trade_date) makes re-runs idempotent: the same day
    loaded twice updates the row instead of duplicating it.
    """
    series = price_data["Time Series (Daily)"]
    rows = [
        (
            ticker,
            trade_date,
            values["1. open"],
            values["2. high"],
            values["3. low"],
            values["4. close"],
            values["5. volume"],
        )
        for trade_date, values in series.items()
    ]
    cur.executemany(
        """
        INSERT INTO daily_prices (ticker, trade_date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, trade_date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            loaded_at = now()
        """,
        rows,
    )
    return len(rows)


def collect(ticker: str, config: dict[str, str], fixture_path: Path | None = None) -> bool:
    """One parameterized entry point (design doc 4.5), so a future scheduler
    (cron / Airflow, project 6) can call this unchanged.

    Order matters: the Postgres upsert is committed first, then the raw
    response is uploaded to S3 on a best-effort basis (a failed upload is
    logged, not raised). In fixture mode the S3 upload is skipped, because a
    saved fixture is not what the live API returned on that day.

    Returns True if fresh price data was written to Postgres. Returns False if
    the run collected nothing: the daily call limit was reached, or the API
    returned no price data. A scheduler uses this through the exit code of
    main(). The S3 upload does not change the result; verify_s3.py checks it."""
    today = dt.datetime.now(dt.timezone.utc).date()

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            calls_today = calls_made_today(cur)
            if calls_today >= DAILY_CALL_LIMIT:
                logger.warning(
                    "Daily call limit (%d) already reached; skipping %s.",
                    DAILY_CALL_LIMIT, ticker,
                )
                return False

            if fixture_path is not None:
                logger.info("Using fixture %s instead of a live API call.", fixture_path)
                with open(fixture_path) as f:
                    raw = json.load(f)
            else:
                raw = fetch_daily_prices(ticker, config["API_KEY"])

            validation = validate_response(raw)
            record_call(cur, ticker, validation.is_valid, validation.error)

            if validation.result is None:
                logger.error(
                    "API call for %s did not return price data (quota/format issue): %s",
                    ticker, validation.error,
                )
                conn.commit()
                return False

            rows_written = upsert_daily_prices(cur, ticker, validation.result)
            conn.commit()
            logger.info("Upserted %d rows for %s.", rows_written, ticker)

            if fixture_path is None:
                upload_raw_to_s3(
                    bucket_name=config["S3_BUCKET_NAME"],
                    ticker=ticker,
                    partition_date=today,
                    raw_json_data=validation.result,
                )
            else:
                logger.info("S3 upload skipped for %s: fixture mode (--fixture).", ticker)

    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect daily stock prices from Alpha Vantage.")
    parser.add_argument("--ticker", default="IBM", help="Stock ticker to collect (default: IBM).")
    parser.add_argument(
        "--fixture",
        type=Path,
        default=None,
        help="Path to a saved API response JSON, used instead of a live call "
        "(development without spending API quota). No raw data is uploaded to S3 in this mode.",
    )
    return parser.parse_args()


def main() -> None:
    """Command-line entry point. Exit code 0 if fresh data was collected, 1 otherwise."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    config = get_config()
    collected = collect(args.ticker, config, fixture_path=args.fixture)
    sys.exit(0 if collected else 1)


if __name__ == "__main__":
    main()
