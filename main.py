import os
import psycopg2
import psycopg2.extras
import pytz
from datetime import datetime
from typing import Dict, Any
from dotenv import load_dotenv
from src.utils import OrderFillEvent, parse_events_from_logs
from flask import Flask, request, jsonify

load_dotenv()

DATABASE_PARAMS = {
    "database": os.getenv("DATABASE_NAME", "postgres"),
    "user": os.getenv("DATABASE_USER", "postgres"),
    "password": os.getenv("DATABASE_PASSWORD", ""),
    "host": os.getenv("DATABASE_HOST", "localhost"),
    "port": os.getenv("DATABASE_PORT", "5432")
}

PORT = os.getenv("PORT", "5000")

app = Flask(__name__)

@app.route('/', methods=['GET'])
def status():
    return jsonify({'status': 'live'})

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    if not isinstance(data, list) or not data:
        return jsonify({'error': "Invalid or missing data"}), 400
    
    data = data[0]
    if data.get("meta", {}).get("err") is not None:
        return jsonify({'error': "Transaction failed"}), 400

    try:    
        handle_transaction(data)
        return jsonify({"message": "Transaction processed"}), 200
    except Exception as e:
        # Log the exception for debugging purposes
        print(f"Exception during transaction processing: {e}")
        return jsonify({'error': "Transaction failed to process"}), 500

def get_db_connection():
    return psycopg2.connect(**DATABASE_PARAMS)

insert_fills_stmt = """
INSERT INTO fills (tx_sig, mpg, product, block_timestamp, slot, inserted_at, taker_side, 
    maker_order_id, quote_size, base_size, maker_trg, taker_trg, price,
    maker_order_nonce, maker_client_order_id, taker_order_nonce, taker_client_order_id,
    taker_order_id, maker_fee, taker_fee)
VALUES %s
ON CONFLICT ON CONSTRAINT fills_pkey DO NOTHING;
"""

def handle_transaction(tr: Dict[str, Any]):
    """
    Checks the given transaction object for fill events and inserts them into the database if found.
    
    :param tr: A dictionary representing the transaction.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if not isinstance(tr, dict):
                print(f"Unexpected type for transaction data: {type(tr)}. Expected a dictionary.")
                return

            if tr.get("meta", {}).get("err") is not None:
                print("Transaction failed. Skipping.")
                return

            events = parse_events_from_logs(tr.get("meta", {}).get("logMessages", []))
            fill_events = [event for event in events if isinstance(event, OrderFillEvent)]

            if fill_events:
                db_fills = [event_to_fill_data(tr, event) for event in fill_events]
                
                try:
                    with conn.cursor() as cursor:
                        psycopg2.extras.execute_values(cursor, insert_fills_stmt, db_fills, page_size=1000)
                    conn.commit()
                    print(f"Inserted {len(db_fills)} fill events.")
                except Exception as e:
                    print(f"Failed to insert fill events due to error: {e}")
                    conn.rollback() 
            else:
                print("No fill events found in transaction.")

def event_to_fill_data(tr: Dict[str, Any], event: OrderFillEvent) -> tuple:
    """
    Converts an OrderFillEvent into a tuple suitable for database insertion.
    
    :param tr: The transaction dictionary.
    :param event: The OrderFillEvent instance.
    :return: A tuple representing the fill event for database insertion.
    """
    sig = tr["transaction"]["signatures"][0]
    block_timestamp = datetime.fromtimestamp(tr["blockTime"], tz=pytz.UTC)
    slot = tr["slot"]
    fill = (
        str(sig),
        str(event.market_product_group),
        event.product,
        block_timestamp,
        slot,
        datetime.now(tz=pytz.UTC),
        event.taker_side.get_name().lower(),
        str(event.maker_order_id),
        event.quote_size,
        event.base_size,
        str(event.maker_trader_risk_group),
        str(event.taker_trader_risk_group),
        event.price,
        event.maker_order_nonce,
        event.maker_client_order_id,
        event.taker_order_nonce,
        event.taker_client_order_id,
        str(event.taker_order_id),
        event.maker_fee,
        event.taker_fee,
    )
    return fill

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=PORT)