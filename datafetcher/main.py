import os
import requests

API_KEY = os.environ["ONEINCH_API_KEY"]
CHAIN_ID = 42161  # Arbitrum One

url = f"https://api.1inch.dev/swap/v6.1/{CHAIN_ID}/tokens"
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "accept": "application/json",
}

def get_available_tokens():
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    tokens = data["tokens"]
    return [addr for addr, info in tokens.items()]
