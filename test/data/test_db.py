import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import MagicMock
from psycopg2.extras import execute_values
from src.data.db import DBService
from src.sql.public import (
    INSERT_CONTRACTS_SQL,
    CREATE_CONTRACTS_TABLE_SQL,
    SELECT_CONTRACTS_SQL,
)

def test_dbservice_store_tokens(mocker):
    # Sample token addresses
    tokens = [
        "0x32eb7902d4134bf98a28b963d26de779af92a212",
        "0x539bde0d7dbd336b79148aa742883198bbf60342",
    ]

    # --- Mock Postgres connection and cursor ---
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = 'UTF8'
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Patch execute_values to track its calls
    mock_execute_values = mocker.patch("src.data.db.execute_values")

    # Create DBService instance with mock connection
    db_service = DBService(mock_conn)

    # Call store_tokens
    db_service.store_tokens(tokens)

    # --- Assertions ---
    # 1. Table creation executed
    create_table_call = mock_cursor.execute.call_args_list[0][0][0]
    assert CREATE_CONTRACTS_TABLE_SQL in create_table_call

    # 2. execute_values called with correct rows
    expected_rows = [(t,) for t in tokens]
    insert_sql_arg = mock_execute_values.call_args[0][1]
    values_arg = mock_execute_values.call_args[0][2]
    assert insert_sql_arg == INSERT_CONTRACTS_SQL
    assert values_arg == expected_rows

    # 3. Commit called
    mock_conn.commit.assert_called()

def test_dbservice_get_tokens(mocker):
    # --- Mock Postgres connection and cursor ---
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = 'UTF8'
    mock_cursor.fetchall.return_value = [
        ('0x32eb7902d4134bf98a28b963d26de779af92a212',),
        ('0x539bde0d7dbd336b79148aa742883198bbf60342',)
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Create DBService instance with mock connection
    db_service = DBService(mock_conn)

    # Call store_tokens
    result = db_service.get_tokens()

    # --- Assertions ---
    # 1. Check that the correct SQL was used
    get_contracts_call = mock_cursor.execute.call_args_list[0][0][0]
    assert SELECT_CONTRACTS_SQL in get_contracts_call 

    # 2. Check that the returned list is correct
    assert result == [
        "0x32eb7902d4134bf98a28b963d26de779af92a212",
        "0x539bde0d7dbd336b79148aa742883198bbf60342",
    ]

@pytest.mark.parametrize("schema", ["backtest", "live"])
def test_dbservice_store_prices(mocker, schema):
    prices = [
        {
            "value": "1900.00",
            "timestamp": "2024-01-01T00:00:00Z",
            "marketCap": "274292310008.21802",
            "totalVolume": "6715146404.608721",
        },
        {
            "value": "1950.50",
            "timestamp": "2024-01-02T00:00:00Z",
            "marketCap": "280000000000.00",
            "totalVolume": "7000000000.00",
        },
    ]
    token_address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = "UTF8"
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_execute_values = mocker.patch("src.data.db.execute_values")

    db_service = DBService(mock_conn)

    # IMPORTANT: schema passed in
    db_service.store_prices(token_address, prices, schema=schema)

    
    executed_sql = [str(c.args[0]) for c in mock_cursor.execute.call_args_list]

    assert any("CREATE SCHEMA IF NOT EXISTS" in s and schema in s for s in executed_sql)
    assert any("CREATE TABLE IF NOT EXISTS" in s and schema in s and ".prices" in s for s in executed_sql)
    assert any("CREATE INDEX IF NOT EXISTS" in s and schema in s and ".prices" in s and "token_address" in s for s in executed_sql)
    assert any("CREATE INDEX IF NOT EXISTS" in s and schema in s and ".prices" in s and "timestamp" in s for s in executed_sql)


    # execute_values called and insert targets the right schema table
    insert_sql_arg = str(mock_execute_values.call_args[0][1])
    values_arg = mock_execute_values.call_args[0][2]

    assert "INSERT INTO" in insert_sql_arg
    assert schema in insert_sql_arg
    assert ".prices" in insert_sql_arg

    # Check rows shape
    assert len(values_arg) == len(prices)
    assert values_arg[0][0] == token_address
    assert values_arg[0][1] == "1900.00"
    assert values_arg[0][2] == "2024-01-01T00:00:00Z"
    assert values_arg[0][3] == "274292310008.21802"
    assert values_arg[0][4] == "6715146404.608721"

    mock_conn.commit.assert_called()

@pytest.mark.parametrize("schema", ["backtest", "live"])
def test_dbservice_get_prices(mocker, schema):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = "UTF8"

    mock_data = [
        (
            "0x32eb7902d4134bf98a28b963d26de779af92a212",
            "0x539bde0d7dbd336b79148aa742883198bbf60342",
            "0.9564383462",
            "2024-04-10 02:00:00.000 +0200",
            "251110346.4283975",
            "37756712.67544287",
            "2026-01-04 12:35:23.576 +0100",
        ),
        (
            "0xabcdefabcdefabcdefabcdefabcdefabcdef",
            "0x1234567890123456789012345678901234567890",
            "1.2345",
            "2025-05-01 10:00:00.000 +0200",
            "123456789.12345",
            "9876543.21",
            "2026-01-01 08:00:00.000 +01:00",
        ),
    ]
    mock_cursor.fetchall.return_value = mock_data
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    db_service = DBService(mock_conn)

    # IMPORTANT: schema passed in
    result = db_service.get_prices(schema=schema)

    executed_sql = str(mock_cursor.execute.call_args[0][0])
    assert "SELECT * FROM" in executed_sql
    assert schema in executed_sql
    assert ".prices" in executed_sql


    expected_df = pd.DataFrame(
        mock_data,
        columns=[
            "uid",
            "token_address",
            "value",
            "timestamp",
            "market_cap",
            "total_volume",
            "created_at",
        ],
    )
    expected_df["value"] = expected_df["value"].astype(float)
    expected_df["market_cap"] = expected_df["market_cap"].astype(float)
    expected_df["total_volume"] = expected_df["total_volume"].astype(float)
    expected_df["timestamp"] = pd.to_datetime(expected_df["timestamp"], utc=True)
    expected_df["created_at"] = pd.to_datetime(expected_df["created_at"], utc=True)

    pd.testing.assert_frame_equal(result, expected_df)

@pytest.mark.parametrize("schema", ["backtest", "live"])
def test_dbservice_get_latest_price_date_returns_datetime(mocker, schema):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = "UTF8"

    # psycopg2 typically returns a Python datetime for timestamptz
    expected_dt = datetime(2026, 1, 4, 11, 35, 23, tzinfo=timezone.utc)
    mock_cursor.fetchone.return_value = (expected_dt,)

    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    db_service = DBService(mock_conn)
    result = db_service.get_latest_price_date(schema=schema)

    assert result == expected_dt

    executed_sql = str(mock_cursor.execute.call_args[0][0])
    assert "SELECT MAX(timestamp)" in executed_sql
    assert schema in executed_sql
    assert ".prices" in executed_sql

@pytest.mark.parametrize("schema", ["backtest", "live"])
def test_dbservice_get_latest_price_date_returns_none_when_max_is_null(mocker, schema):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = "UTF8"

    # Empty table -> MAX(timestamp) is NULL -> (None,)
    mock_cursor.fetchone.return_value = (None,)
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    db_service = DBService(mock_conn)
    result = db_service.get_latest_price_date(schema=schema)

    assert result is None

    executed_sql = str(mock_cursor.execute.call_args[0][0])
    assert "SELECT MAX(timestamp)" in executed_sql
    assert schema in executed_sql
    assert ".prices" in executed_sql


@pytest.mark.parametrize("schema", ["backtest", "live"])
def test_dbservice_get_latest_price_date_returns_none_when_fetchone_is_none(mocker, schema):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = "UTF8"

    # Defensive branch: cursor.fetchone() unexpectedly returns None
    mock_cursor.fetchone.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    db_service = DBService(mock_conn)
    result = db_service.get_latest_price_date(schema=schema)

    assert result is None


@pytest.mark.parametrize("schema", ["backtest", "live"])
def test_dbservice_get_latest_price_date_raises_and_logs_on_db_error(mocker, schema):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = "UTF8"

    mock_cursor.execute.side_effect = Exception("db exploded")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Patch the module-level logger used inside DBService
    mock_logger = mocker.patch("src.data.db.logger")

    db_service = DBService(mock_conn)

    with pytest.raises(Exception, match="db exploded"):
        db_service.get_latest_price_date(schema=schema)

    mock_logger.exception.assert_called_once()

def test_dbservice_get_prices_distinct_tokens_default_schema(mocker):
    # --- Mock Postgres connection and cursor ---
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = "UTF8"
    mock_cursor.fetchall.return_value = [
        ("0x32eb7902d4134bf98a28b963d26de779af92a212",),
        ("0x539bde0d7dbd336b79148aa742883198bbf60342",),
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    db_service = DBService(mock_conn)

    # Call method (default schema is "backtest")
    result = db_service.get_prices_distinct_tokens()

    # --- Assertions ---
    # 1) execute() was called once
    assert mock_cursor.execute.call_count == 1

    # 2) Validate the SQL contains the expected structure + default schema
    executed_query = mock_cursor.execute.call_args_list[0][0][0]
    # psycopg2.sql.SQL/Composed isn't a plain string; str() is usually fine for assertions
    executed_query_str = str(executed_query)

    assert "SELECT DISTINCT token_address" in executed_query_str
    assert "FROM" in executed_query_str
    assert "backtest" in executed_query_str
    assert ".prices" in executed_query_str

    # 3) Returned values are flattened list of strings
    assert result == [
        "0x32eb7902d4134bf98a28b963d26de779af92a212",
        "0x539bde0d7dbd336b79148aa742883198bbf60342",
    ]

def test_dbservice_get_prices_distinct_tokens_custom_schema(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = "UTF8"
    mock_cursor.fetchall.return_value = [
        ("0xaaa",),
        ("0xbbb",),
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    db_service = DBService(mock_conn)

    result = db_service.get_prices_distinct_tokens(schema="prod")

    executed_query = mock_cursor.execute.call_args_list[0][0][0]
    executed_query_str = str(executed_query)

    assert "SELECT DISTINCT token_address" in executed_query_str
    assert "prod" in executed_query_str
    assert ".prices" in executed_query_str

    assert result == ["0xaaa", "0xbbb"]


def test_dbservice_get_prices_distinct_tokens_raises_and_logs(mocker):
    logger_mock = mocker.patch("src.data.db.logger")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.connection.encoding = "UTF8"
    mock_cursor.execute.side_effect = Exception("db is down")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    db_service = DBService(mock_conn)

    with pytest.raises(Exception, match="db is down"):
        db_service.get_prices_distinct_tokens()

    logger_mock.exception.assert_called_once_with(
        "Failed to get all distinct token addresses in price table"
    )
