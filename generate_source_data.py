import os
import random
import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
load_dotenv()

# Configure local logging profile
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_postgres_connection():
    """Builds a connection map back to your local exposed Postgres port"""
    return psycopg2.connect(
        host="127.0.0.1",  # Localhost for containerized Postgres access
        port=5433,  # Exposed local mapping port from docker-compose.yml
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB")
    )

def init_source_schema():
    """Creates the structural tables inside the Postgres container layer"""
    commands = (
        """
        DROP TABLE IF EXISTS raw_transactions;
        """,
        """
        CREATE TABLE raw_transactions (
            transaction_id SERIAL PRIMARY KEY,
            customer_id INT NOT NULL,
            product_category VARCHAR(100) NOT NULL,
            item_count INT NOT NULL,
            purchase_amount NUMERIC(10, 2) NOT NULL,
            payment_method VARCHAR(50) NOT NULL,
            store_location VARCHAR(100) NOT NULL,
            transaction_timestamp TIMESTAMP NOT NULL
        );
        """
    )
    
    conn = None
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
        logging.info("Source database tables initialized successfully.")
    except Exception as e:
        logging.error(f"Error executing schema compilation: {e}")
        raise e
    finally:
        if conn:
            conn.close()

def seed_transactional_records(num_records=5000):
    """Generates random high-fidelity transactional entries for modeling"""
    categories = ['Electronics', 'Apparel', 'Home & Kitchen', 'Automotive', 'Books', 'Beauty']
    methods = ['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Bank Transfer']
    locations = ['London', 'Birmingham', 'Manchester', 'Glasgow', 'Liverpool', 'Bristol']
    
    records = []
    base_time = datetime.now() - timedelta(days=90)
    
    logging.info(f"Generating {num_records} mock records for ingestion testing...")
    
    for i in range(num_records):
        customer_id = random.randint(10000, 99999)
        category = random.choice(categories)
        count = random.randint(1, 5)
        # Determine average cost multipliers based on categories
        base_price = 150.00 if category == 'Electronics' else 25.00
        amount = round(base_price * count * random.uniform(0.8, 1.5), 2)
        method = random.choice(methods)
        location = random.choice(locations)
        # Smooth timestamps backward through history
        timestamp = base_time + timedelta(minutes=random.randint(1, 129600))
        
        records.append((customer_id, category, count, amount, method, location, timestamp))
        
    # Batch execute execution values over network stream
    query = """
        INSERT INTO raw_transactions 
        (customer_id, product_category, item_count, purchase_amount, payment_method, store_location, transaction_timestamp)
        VALUES %s
    """
    
    conn = None
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        execute_values(cur, query, records)
        conn.commit()
        cur.close()
        logging.info(f"Successfully injected {num_records} transactions into 'raw_transactions'.")
    except Exception as e:
        logging.error(f"Error committing transaction seed batch: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Wait for containers to be up before running this script locally
    init_source_schema()
    seed_transactional_records()