import os

class OneInchSettings:
    API_KEY: str = os.environ.get("ONEINCH_API_KEY", "")
    CHAIN_ID: int = int(os.environ.get("ONEINCH_CHAIN_ID", 42161))

    @property
    def get_tokens_url(self) -> str:
        return f"https://api.1inch.dev/swap/v6.1/{self.CHAIN_ID}/tokens"

    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.API_KEY}",
            "accept": "application/json"
        }


# Singleton instances
oneinch_settings = OneInchSettings()

