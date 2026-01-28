from datetime import datetime, timedelta, timezone
import psycopg2

import src.data as data
from src.bot.check_tokens import check_new_tokens
from src.db_config import DB_CONFIG

DEFAULT_LOOKBACK_DAYS = 120


def get_live_latest_timestamp(schema: str = "live"):
    """Return latest timestamp in live.prices or None if empty."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        db_service = data.DBService(conn)
        return db_service.get_latest_price_date(schema=schema)


def get_prices():
    # 1) check if there are new tokens
    check_new_tokens()

    # 2) determine start date
    latest_ts = get_live_latest_timestamp(schema="live")

    if latest_ts is None:
        start_date = datetime.now(timezone.utc) - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    else:
        start_date = latest_ts

    # 3) fetch historical prices
    tokens = data.get_available_tokens()
    data.fetch_historical_prices(tokens, start_date=start_date)

