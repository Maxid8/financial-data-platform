"""Unit tests for API_call.py.

PostgreSQL, the Alpha Vantage API and S3 are replaced by mocks, so the suite
runs without a database, network access or API quota.
"""

import json
import urllib.error
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pytest

from API_call import (
    calls_made_today,
    collect,
    fetch_daily_prices,
    main,
    record_call,
    upsert_daily_prices,
    validate_response,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict:
    with open(FIXTURES_DIR / name) as f:
        return json.load(f)


def test_validate_response_accepts_a_successful_response():
    response = load_fixture("daily_series_success.json")

    result = validate_response(response)

    assert result.is_valid is True
    assert result.result == response
    assert result.error is None


def test_validate_response_rejects_an_invalid_api_key_response():
    response = load_fixture("invalid_api_key.json")

    result = validate_response(response)

    assert result.is_valid is False
    assert result.result is None
    assert result.error == response


def test_record_call_inserts_a_successful_call():
    mock_cursor = MagicMock()

    record_call(mock_cursor, "IBM", True, None)

    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO api_calls (ticker, success, error_reason) VALUES (%s, %s, %s)",
        ("IBM", True, None),
    )


def test_record_call_inserts_a_failed_call():
    mock_cursor = MagicMock()

    response = load_fixture("invalid_api_key.json")

    record_call(mock_cursor, "IBM", False, response)

    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO api_calls (ticker, success, error_reason) VALUES (%s, %s, %s)",
        ("IBM", False, response["Information"]),
    )


def test_upsert_daily_prices_builds_expected_rows_and_returns_row_count():
    mock_cursor = MagicMock()
    price_data = {
        "Time Series (Daily)": {
            "2026-09-15": {
                "1. open": "100.0000",
                "2. high": "105.0000",
                "3. low": "99.0000",
                "4. close": "104.0000",
                "5. volume": "123456",
            },
            "2026-09-14": {
                "1. open": "98.0000",
                "2. high": "101.0000",
                "3. low": "97.5000",
                "4. close": "100.0000",
                "5. volume": "98765",
            },
        }
    }

    rows_written = upsert_daily_prices(mock_cursor, "IBM", price_data)

    sql, rows = mock_cursor.executemany.call_args.args

    expected_rows = [
        ("IBM", "2026-09-15", "100.0000", "105.0000", "99.0000", "104.0000", "123456"),
        ("IBM", "2026-09-14", "98.0000", "101.0000", "97.5000", "100.0000", "98765"),
    ]

    assert rows == expected_rows
    assert rows_written == len(expected_rows)
    assert "ON CONFLICT (ticker, trade_date)" in sql


def test_calls_made_today_returns_count_from_query():
    mock_cursor = MagicMock()

    mock_cursor.fetchone.return_value = (3,)

    assert calls_made_today(mock_cursor) == 3


def test_collect_returns_false_and_writes_nothing_when_daily_limit_reached():
    with patch("API_call.get_connection") as mock_get_connection:
        mock_cur = (
            mock_get_connection.return_value
            .__enter__.return_value
            .cursor.return_value
            .__enter__.return_value
        )

        mock_cur.fetchone.return_value = (25,)

        result = collect("IBM", {"API_KEY": "dummy"})

        assert result is False
        mock_cur.executemany.assert_not_called()


def test_collect_returns_true_and_writes_data_when_under_daily_limit():
    with patch("API_call.get_connection") as mock_get_connection:
        mock_cur = (
            mock_get_connection.return_value
            .__enter__.return_value
            .cursor.return_value
            .__enter__.return_value
        )

        mock_cur.fetchone.return_value = (0,)

        result = collect("IBM", {"API_KEY": "dummy"}, fixture_path=FIXTURES_DIR / "daily_series_success.json")

        assert result is True
        mock_cur.executemany.assert_called_once()


def test_collect_skips_s3_upload_in_fixture_mode():
    with patch("API_call.get_connection") as mock_get_connection, \
            patch("API_call.upload_raw_to_s3") as mock_upload:
        mock_cur = (
            mock_get_connection.return_value
            .__enter__.return_value
            .cursor.return_value
            .__enter__.return_value
        )
        mock_cur.fetchone.return_value = (0,)

        collect("IBM", {"API_KEY": "dummy"}, fixture_path=FIXTURES_DIR / "daily_series_success.json")

    mock_upload.assert_not_called()


def test_collect_uploads_raw_data_to_s3_in_live_mode():
    price_data = load_fixture("daily_series_success.json")
    config = {"API_KEY": "dummy", "S3_BUCKET_NAME": "test-bucket"}

    with patch("API_call.get_connection") as mock_get_connection, \
            patch("API_call.fetch_daily_prices", return_value=price_data), \
            patch("API_call.upload_raw_to_s3") as mock_upload:
        mock_cur = (
            mock_get_connection.return_value
            .__enter__.return_value
            .cursor.return_value
            .__enter__.return_value
        )
        mock_cur.fetchone.return_value = (0,)

        collect("IBM", config)

    mock_upload.assert_called_once_with(
        bucket_name="test-bucket",
        ticker="IBM",
        partition_date=ANY,
        raw_json_data=price_data,
    )


def test_collect_returns_false_and_skips_s3_upload_when_api_response_is_invalid():
    error_response = load_fixture("invalid_api_key.json")
    config = {"API_KEY": "dummy", "S3_BUCKET_NAME": "test-bucket"}

    with patch("API_call.get_connection") as mock_get_connection, \
            patch("API_call.fetch_daily_prices", return_value=error_response), \
            patch("API_call.upload_raw_to_s3") as mock_upload:
        mock_cur = (
            mock_get_connection.return_value
            .__enter__.return_value
            .cursor.return_value
            .__enter__.return_value
        )
        mock_cur.fetchone.return_value = (0,)

        result = collect("IBM", config)

    assert result is False
    mock_upload.assert_not_called()


# A scheduler only sees the exit code, so an empty run must not exit with 0.
@pytest.mark.parametrize(
    "collected, expected_exit_code",
    [(True, 0), (False, 1)],
    ids=["data_collected", "nothing_collected"],
)
def test_main_exit_code_reflects_collect_result(collected, expected_exit_code):
    with patch("API_call.parse_args") as mock_parse_args, \
            patch("API_call.get_config", return_value={}), \
            patch("API_call.collect", return_value=collected):
        mock_parse_args.return_value.ticker = "IBM"
        mock_parse_args.return_value.fixture = None

        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == expected_exit_code


def test_fetch_daily_prices_returns_parsed_json_on_success():
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"Time Series (Daily)": {}}'
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = fetch_daily_prices("IBM", "dummy_key")

        assert result == {"Time Series (Daily)": {}}


def test_fetch_daily_prices_retries_after_network_error_then_succeeds():
    with patch("urllib.request.urlopen") as mock_urlopen, patch("time.sleep"):
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"Time Series (Daily)": {}}'

        mock_success = MagicMock()
        mock_success.__enter__.return_value = mock_response

        mock_urlopen.side_effect = [
            urllib.error.URLError("network down"),
            mock_success,
        ]

        result = fetch_daily_prices("IBM", "dummy_key")

    assert result == {"Time Series (Daily)": {}}
    assert mock_urlopen.call_count == 2


def test_fetch_daily_prices_raises_after_exhausting_retries():
    with patch("urllib.request.urlopen") as mock_urlopen, patch("time.sleep"):
        mock_urlopen.side_effect = urllib.error.URLError("network down")

        with pytest.raises(RuntimeError):
            fetch_daily_prices("IBM", "dummy_key")

    assert mock_urlopen.call_count == 3
