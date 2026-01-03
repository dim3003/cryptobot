import logging
from datetime import datetime
from typing import Optional
from src.db import DBService
from src.fetcher import get_token_prices

logger = logging.getLogger(__name__)

# Default date range: from Ethereum launch (2015-07-30) to now
# This should cover the "furthest possible" historical data
DEFAULT_START_DATE = datetime(2015, 7, 30)
DEFAULT_END_DATE = datetime.now()


def fetch_and_store_all_historical_prices(
    db_service: DBService,
    network: str = "arb-mainnet",
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> None:
    """
    Fetch and store all historical prices for all tokens in the contracts table.
    
    Args:
        db_service: DBService instance with active database connection
        network: Network identifier (default "eth-mainnet")
        start_date: Start date for historical prices (default: Ethereum launch date)
        end_date: End date for historical prices (default: current date)
    
    This function:
    1. Gets all token addresses from the contracts table
    2. For each token, fetches historical prices from the API
    3. Stores the prices in the prices table
    """
    # Use default dates if not provided
    if start_date is None:
        start_date = DEFAULT_START_DATE
    if end_date is None:
        end_date = DEFAULT_END_DATE
    
    # Get all tokens from database
    tokens = db_service.get_tokens()
    
    if not tokens:
        logger.info("No tokens found in database. Skipping price fetch.")
        return
    
    logger.info(f"Found {len(tokens)} tokens. Fetching historical prices...")
    
    total_prices_stored = 0
    
    # Process each token
    for token_address in tokens:
        try:
            logger.info(f"Fetching prices for token: {token_address}")
            
            # Fetch historical prices for this token
            prices = list(get_token_prices(
                network=network,
                address=token_address,
                start=start_date,
                end=end_date
            ))
            
            if prices:
                # Store prices in database
                db_service.store_prices(token_address, prices)
                total_prices_stored += len(prices)
                logger.info(f"Stored {len(prices)} prices for token {token_address}")
            else:
                logger.warning(f"No prices found for token {token_address}")
                
        except Exception as e:
            logger.error(f"Error processing token {token_address}: {e}")
            # Continue with next token even if one fails
            continue
    
    logger.info(f"Completed. Stored {total_prices_stored} total price records.")
