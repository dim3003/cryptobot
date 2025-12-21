import os
import requests

API_KEY = os.environ["ONEINCH_API_KEY"]
CHAIN_ID = 42161  # Arbitrum One

url = f"https://api.1inch.dev/swap/v6.1/{CHAIN_ID}/tokens"
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "accept": "application/json",
}

resp = requests.get(url, headers=headers, timeout=30)
resp.raise_for_status()
data = resp.json()

tokens = data["tokens"]  # dict: address -> metadata
print("Arbitrum token count:", len(tokens))

# Example: normalize to a list
token_list = [
    {
        "address": addr,
        "symbol": info.get("symbol"),
        "name": info.get("name"),
        "decimals": info.get("decimals"),
        "logoURI": info.get("logoURI"),
        "tags": info.get("tags", [])
    }
    for addr, info in tokens.items()
]

print(token_list[:10])

def get_weather(temp):
    if (temp) > 20:
        return "hot"
    else:
        return "cold"
