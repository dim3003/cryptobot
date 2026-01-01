import os
import requests
from typing import Generator
from src.config import oneinch_settings

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
