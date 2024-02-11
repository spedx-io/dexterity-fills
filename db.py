import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_PARAMS = {
    "database": os.getenv("DATABASE_NAME", "postgres"),
    "user": os.getenv("DATABASE_USER", "postgres"),
    "password": os.getenv("DATABASE_PASSWORD", ""),
    "host": os.getenv("DATABASE_HOST", "localhost"),
    "port": os.getenv("DATABASE_PORT", "5432")
}

def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database using parameters defined in DATABASE_PARAMS.
    
    :return: A connection object to the PostgreSQL database.
    """
    return psycopg2.connect(**DATABASE_PARAMS)

def create_fills_table():
    """
    Creates the 'fills' table in the PostgreSQL database if it does not already exist.
    This table is designed to store fill events from transactions, where 'tx_sig' can be shared between multiple fills.
    """
    create_table_stmt = """
    CREATE TABLE IF NOT EXISTS fills (
        fill_id SERIAL PRIMARY KEY,
        tx_sig VARCHAR NOT NULL,
        mpg VARCHAR,
        product VARCHAR,
        block_timestamp TIMESTAMP WITH TIME ZONE,
        slot BIGINT,
        inserted_at TIMESTAMP WITH TIME ZONE,
        taker_side VARCHAR,
        maker_order_id VARCHAR,
        quote_size NUMERIC,
        base_size NUMERIC,
        maker_trg VARCHAR,
        taker_trg VARCHAR,
        price NUMERIC,
        maker_order_nonce BIGINT,
        maker_client_order_id VARCHAR,
        taker_order_nonce BIGINT,
        taker_client_order_id VARCHAR,
        taker_order_id VARCHAR,
        maker_fee NUMERIC,
        taker_fee NUMERIC,
        UNIQUE(tx_sig, maker_order_id, taker_order_id)
    );
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_stmt)
            print("The 'fills' table has been successfully created or already exists.")
        conn.commit()
