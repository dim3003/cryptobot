import pytest
from unittest.mock import MagicMock
from datetime import datetime, timedelta

from src.data.historical_prices import (
    fetch_historical_prices,
    MAX_DAYS_PER_REQUEST,
    DEFAULT_START_DATE,
    DEFAULT_END_DATE,
)


def test_fetch_historical_prices_fetches_for_each_token(mocker):
    """
    Test that the function fetches historical prices for each token
    and returns them keyed by token address.
    """
    tokens = [
        "0x32eb7902d4134bf98a28b963d26de779af92a212",
        "0x539bde0d7dbd336b79148aa742883198bbf60342",
    ]

    prices_token1 = [
        {"value": "1900.00", "timestamp": "2024-01-01T00:00:00Z", "marketCap": "1", "totalVolume": "1"},
        {"value": "1950.50", "timestamp": "2024-01-02T00:00:00Z", "marketCap": "2", "totalVolume": "2"},
    ]
    prices_token2 = [
        {"value": "1.00", "timestamp": "2024-01-01T00:00:00Z", "marketCap": "3", "totalVolume": "3"},
    ]

    # Mock get_token_prices to return different prices per token
    def mock_get_token_prices(network, address, start, end):
        if address == tokens[0]:
            return iter(prices_token1)
        if address == tokens[1]:
            return iter(prices_token2)
        return iter([])

    mock_get_prices = mocker.patch(
        "src.data.historical_prices.get_token_prices",
        side_effect=mock_get_token_prices,
    )

    # Use a small range to ensure exactly one batch per token
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 10)

    result = fetch_historical_prices(tokens, start_date=start_date, end_date=end_date)

    # Should call at least once per token (may be multiple if range is large)
    assert mock_get_prices.call_count >= len(tokens)

    # Returned dict should contain both tokens
    assert set(result.keys()) == set(tokens)

    # Returned prices should include expected values
    assert {p.get("value") for p in result[tokens[0]]} == {"1900.00", "1950.50"}
    assert {p.get("value") for p in result[tokens[1]]} == {"1.00"}


def test_fetch_historical_prices_with_empty_tokens(mocker):
    """
    Test that the function handles empty token list gracefully.
    """
    mock_get_prices = mocker.patch("src.data.historical_prices.get_token_prices")

    result = fetch_historical_prices([])

    assert result == {}
    mock_get_prices.assert_not_called()


def test_fetch_historical_prices_uses_correct_date_range_batching(mocker):
    """
    Test that the function uses correct batching boundaries.
    """
    token = "0x32eb7902d4134bf98a28b963d26de779af92a212"
    prices = [{"value": "1900.00", "timestamp": "2024-01-01T00:00:00Z"}]

    mock_get_prices = mocker.patch(
        "src.data.historical_prices.get_token_prices",
        return_value=iter(prices),
    )

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)

    result = fetch_historical_prices([token], start_date=start_date, end_date=end_date)

    # Should have data returned for token
    assert token in result

    # Verify at least one call occurred
    assert mock_get_prices.call_count > 0

    # First call start should be exactly start_date
    first_call = mock_get_prices.call_args_list[0]
    assert first_call.kwargs["start"] == start_date

    # First batch end should be start_date + (MAX_DAYS_PER_REQUEST - 1) days, unless end_date is earlier
    expected_first_end = min(start_date + timedelta(days=MAX_DAYS_PER_REQUEST - 1), end_date)
    assert first_call.kwargs["end"] == expected_first_end

    # Last call end should be exactly end_date (because loop is inclusive)
    last_call = mock_get_prices.call_args_list[-1]
    assert last_call.kwargs["end"] == end_date


def test_fetch_historical_prices_defaults_to_eth_launch_and_now(mocker):
    """
    Test that default start_date/end_date are applied when not provided.
    """
    token = "0x32eb7902d4134bf98a28b963d26de779af92a212"

    mock_get_prices = mocker.patch(
        "src.data.historical_prices.get_token_prices",
        return_value=iter([]),
    )

    fetch_historical_prices([token])

    # First call should use defaults
    first_call = mock_get_prices.call_args_list[0]
    assert first_call.kwargs["start"] == DEFAULT_START_DATE
    assert first_call.kwargs["end"] <= DEFAULT_END_DATE

