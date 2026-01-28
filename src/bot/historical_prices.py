import psycopg2

def check_latest_entry():
    with psycopg2.connect(**DB_CONFIG) as conn:
        db_service = data.DBService(conn)
        db_prices = set(db_service.get_prices("live"))

    
