import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone, timedelta


def test_get_live_latest_timestamp_returns_latest_ts(mocker):
    # Arrange
    expected_ts = datetime(2026, 1, 10, 8, 30, tzinfo=timezone.utc)

    # psycopg2 connection context manager
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mocker.patch("psycopg2.connect", return_value=mock_conn)

    # DBService instance
    mock_db_service = MagicMock()
    mock_db_service.get_latest_price_date.return_value = expected_ts
    mocker.patch("src.data.DBService", return_value=mock_db_service)

    # Act
    from src.bot.historical_prices import get_live_latest_timestamp
    result = get_live_latest_timestamp(schema="live")

    # Assert
    assert result == expected_ts
    mock_db_service.get_latest_price_date.assert_called_once_with(schema="live")


def test_get_prices_uses_fallback_when_no_latest(mocker):
    # Arrange
    fixed_now = datetime(2026, 1, 28, 10, 0, 0, tzinfo=timezone.utc)

    # Patch datetime.now() in the module under test
    # IMPORTANT: patch the module's datetime, not the global datetime import
    mock_datetime = mocker.patch("src.bot.historical_prices.datetime")
    mock_datetime.now.return_value = fixed_now

    mocker.patch("src.bot.historical_prices.DEFAULT_LOOKBACK_DAYS", 120)
    mocker.patch("src.bot.historical_prices.get_live_latest_timestamp", return_value=None)

    mock_check = mocker.patch("src.bot.historical_prices.check_new_tokens")
    mock_get_tokens = mocker.patch("src.data.get_available_tokens", return_value=["t1", "t2"])
    mock_fetch = mocker.patch("src.data.fetch_historical_prices")

    # Act
    from src.bot.historical_prices import get_prices
    get_prices()

    # Assert
    mock_check.assert_called_once()
    mock_get_tokens.assert_called_once()

    expected_start = fixed_now - timedelta(days=120)
    mock_fetch.assert_called_once_with(["t1", "t2"], start_date=expected_start)


def test_get_prices_uses_latest_timestamp_when_present(mocker):
    # Arrange
    latest_ts = datetime(2026, 1, 10, 8, 30, tzinfo=timezone.utc)

    mocker.patch("src.bot.historical_prices.get_live_latest_timestamp", return_value=latest_ts)

    mock_check = mocker.patch("src.bot.historical_prices.check_new_tokens")
    mock_get_tokens = mocker.patch("src.data.get_available_tokens", return_value=["a"])
    mock_fetch = mocker.patch("src.data.fetch_historical_prices")

    # Act
    from src.bot.historical_prices import get_prices
    get_prices()

    # Assert
    mock_check.assert_called_once()
    mock_get_tokens.assert_called_once()
    mock_fetch.assert_called_once_with(["a"], start_date=latest_ts)
