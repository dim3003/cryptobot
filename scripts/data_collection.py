#!/usr/bin/env python3
"""
Data collection script.

Fetches tokens from 1inch API and stores historical prices from Alchemy in the database.
"""
import psycopg2
import logging

# Configure logging FIRST, before importing other modules
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

from src.data import DBService, get_available_tokens, fetch_and_store_all_historical_prices
from src.db_config import DB_CONFIG
from src.sql import CREATE_CONTRACTS_TABLE_SQL, SELECT_COUNT_CONTRACTS

logger = logging.getLogger(__name__)

def main():
    """Main data collection workflow."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        db_service = DBService(conn)

        # Ensure contracts table exists
        with conn.cursor() as curs:
            curs.execute(CREATE_CONTRACTS_TABLE_SQL)
            curs.execute(SELECT_COUNT_CONTRACTS)
            count_before = curs.fetchone()[0]

        # Fetch + store all tokens (ON CONFLICT handles duplicates)
        logger.info("Fetching and storing all contracts...")
        tokens = list(get_available_tokens())
        db_service.store_tokens(tokens)

        with conn.cursor() as curs:
            curs.execute(SELECT_COUNT_CONTRACTS)
            count_after = curs.fetchone()[0]

        logger.info(
            "Contracts: %s -> %s (stored %s new tokens)",
            count_before,
            count_after,
            len(tokens),
        )

        # Fetch historical prices (no DB work here)
        logger.info("Fetching historical prices for %s tokens...", len(tokens))
        prices_by_token = fetch_all_historical_prices(
            tokens=tokens,
            network="arb-mainnet",
            # start_date=...,  # optional override
            # end_date=...,    # optional override
        )
        logger.info("Completed fetching historical prices.")

        # Store prices in DB AFTER fetching
        logger.info("Storing historical prices in DB...")
        total_prices = 0
        for token_address, prices in prices_by_token.items():
            if not prices:
                continue
            db_service.store_prices(token_address, prices)
            total_prices += len(prices)

        logger.info("Stored %s total price records.", total_prices)
        logger.info("Completed workflow.")


if __name__ == "__main__":
    main()

