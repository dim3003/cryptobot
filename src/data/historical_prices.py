import logging
from datetime import datetime, timedelta
from typing import Iterable, Dict, List, Optional, Any

from src.data.fetcher import get_token_prices

logger = logging.getLogger(__name__)

# Default date range: from Ethereum launch (2015-07-30) to now
DEFAULT_START_DATE = datetime(2015, 7, 30)
DEFAULT_END_DATE = datetime.now()

MAX_DAYS_PER_REQUEST = 365


def fetch_historical_prices(
    tokens: Iterable[str],
    network: str = "arb-mainnet",
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Dict[str, List[Any]]:
    """
    Fetch historical prices for the given tokens.

    Args:
        tokens: Iterable of token addresses
        network: Network identifier
        start_date: Start date (default: Ethereum launch date)
        end_date: End date (default: now)

    Returns:
        Dict[token_address, list_of_prices]
    """
    if start_date is None:
        start_date = DEFAULT_START_DATE
    if end_date is None:
        end_date = DEFAULT_END_DATE

    token_list = list(tokens)
    if not token_list:
        logger.info("No tokens provided. Skipping price fetch.")
        return {}

    logger.info(f"Found {len(token_list)} tokens. Fetching historical prices...")

    prices_by_token: Dict[str, List[Any]] = {}
    total_tokens = len(token_list)

    for i, token_address in enumerate(token_list, start=1):
        try:
            logger.info(f"[{i}/{total_tokens}] Fetching prices for token: {token_address}")

            all_prices: List[Any] = []
            current_start = start_date
            batch_num = 1

            while current_start <= end_date:
                current_end = min(
                    current_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1),
                    end_date,
                )

                logger.info(
                    f"Fetching batch {batch_num} for token {token_address}: "
                    f"{current_start.date()} to {current_end.date()}"
                )

                batch_prices = list(
                    get_token_prices(
                        network=network,
                        address=token_address,
                        start=current_start,
                        end=current_end,
                    )
                )

                if batch_prices:
                    all_prices.extend(batch_prices)
                    logger.info(
                        f"Fetched {len(batch_prices)} prices for batch {batch_num} "
                        f"of token {token_address}"
                    )
                else:
                    logger.warning(f"No prices found for batch {batch_num} of token {token_address}")

                current_start = current_end + timedelta(days=1)
                batch_num += 1

            prices_by_token[token_address] = all_prices

        except Exception as e:
            logger.error(f"Error processing token {token_address}: {e}")
            continue

    logger.info("Completed fetching historical prices.")
    return prices_by_token

