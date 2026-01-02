import os
import requests
from typing import Generator
from datetime import datetime
from typing import Union
from src.config import oneinch_settings, alchemy_settings

def get_available_tokens() -> Generator[str, None, None]:
    """
        Fetch available token addresses from 1inch and yield them one by one.

        Yields:
            str: The token address as a string.

        Raises:
        RuntimeError: If the API response does not contain a 'tokens' key.
        requests.HTTPError: If the API request fails.
    """
    resp = requests.get(oneinch_settings.get_tokens_url, headers=oneinch_settings.headers)
    resp.raise_for_status()
    data = resp.json()
    tokens = data.get("tokens")
    if not tokens:
        raise RuntimeError("Unexpected API response: missing 'tokens' key")
    for addr in tokens.keys():
        yield addr


def get_token_prices(
    network: str = "eth-mainnet",
    address: str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    start: Union[datetime, float] = 1704067200,
    end: Union[datetime, float] = 1706745599,
) -> Generator[dict, None, None]:
    """
    Yield historical token prices from the API.

    Args:
        network (str): Network identifier (default "eth-mainnet").
        address (str): Token contract address (default USDC).
        start (datetime | float): Start time (datetime or epoch timestamp).
        end (datetime | float): End time (datetime or epoch timestamp).

    Yields:
        dict: A dictionary representing a price point.
    """
    # Convert timestamps to ISO 8601 if needed
    def to_iso(dt: Union[datetime, float]) -> str:
        if isinstance(dt, float) or isinstance(dt, int):
            return datetime.utcfromtimestamp(dt).isoformat() + "Z"
        return dt.isoformat() + "Z"

    payload = {
        "network": network,
        "address": address,
        "startTime": to_iso(start),
        "endTime": to_iso(end)
    }

    resp = requests.post(
        alchemy_settings.get_token_historical_prices_url,
        json=payload,
        headers=alchemy_settings.headers
    )
    resp.raise_for_status()

    data = resp.json().get("data")
    if not data:
        raise RuntimeError("Unexpected API response: missing 'data' key")

    prices = data.get("prices")
    if not prices:
        raise RuntimeError("Unexpected API response: missing 'prices' key")

    for price in prices:
        yield price

