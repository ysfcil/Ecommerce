#!/usr/bin/env python3
"""
Simple Kafka consumer that writes events to PostgreSQL with dimension population.
"""
import json
import os
import logging
import psycopg2
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "data_engineer")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "supersecretpassword")
DB_NAME = os.environ.get("DB_NAME", "ecommerce_warehouse")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

def connect_db():
    """Connect to PostgreSQL"""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        logger.info(f"✅ Connected to PostgreSQL at {DB_HOST}:{DB_PORT}")
        return conn
    except Exception as e:
        logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
        raise

def extract_user_id(user_id_str):
    """Extract numeric ID from 'usr_12345' format"""
    if not user_id_str:
        return None
    try:
        if isinstance(user_id_str, str) and "_" in user_id_str:
            return int(user_id_str.split("_")[1])
        else:
            return int(user_id_str)
    except (ValueError, IndexError, TypeError):
        return None

def extract_product_id(product_id_str):
    """Extract numeric ID from 'prod_5678' format"""
    if not product_id_str:
        return None
    try:
        if isinstance(product_id_str, str) and "_" in product_id_str:
            return int(product_id_str.split("_")[1])
        else:
            return int(product_id_str)
    except (ValueError, IndexError, TypeError):
        return None

def upsert_user(cursor, user_id_num, user_id_str, user_obj):
    """Insert or update user in dim_users"""
    try:
        name = user_obj.get("name", "Unknown")
        email = user_obj.get("email", "unknown@example.com")
        country = user_obj.get("location", {}).get("country", "US")
        
        query = """
            INSERT INTO dim_users (user_id, user_key, name, email, country)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING;
        """
        cursor.execute(query, (user_id_num, user_id_str, name, email, country))
    except Exception as e:
        logger.debug(f"Could not upsert user {user_id_num}: {e}")

def upsert_product(cursor, product_id_num, product_id_str, product_obj):
    """Insert or update product in dim_products"""
    try:
        title = product_obj.get("title", "Product")
        category = product_obj.get("category", "General")
        price = float(product_obj.get("price", 0.0))
        
        query = """
            INSERT INTO dim_products (product_id, product_key, title, category, price)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING;
        """
        cursor.execute(query, (product_id_num, product_id_str, title, category, price))
    except Exception as e:
        logger.debug(f"Could not upsert product {product_id_num}: {e}")

def consume_and_write():
    """Consume Kafka messages and write to PostgreSQL"""
    try:
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
        consumer = KafkaConsumer(
            'ecommerce_clickstream',
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset='earliest',
            group_id='warehouse-logging',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            session_timeout_ms=30000,
            max_poll_interval_ms=300000
        )
        logger.info("✅ Connected to Kafka")
        
        conn = connect_db()
        cursor = conn.cursor()
        
        inserted_count = 0
        debug_count = 0
        
        for message in consumer:
            try:
                event = message.value
                
                # Extract nested IDs from user and product objects
                user_obj = event.get("user", {})
                product_obj = event.get("product", {})
                
                user_id_str = user_obj.get("user_id")
                device_parent = user_obj.get("device")
                device_type = device_parent.get("type")
                product_id_str = product_obj.get("product_id")
                
                user_id_num = extract_user_id(user_id_str)
                product_id_num = extract_product_id(product_id_str)
                
                if debug_count < 5:
                    logger.info(f"DEBUG {debug_count}: user_id_str={user_id_str} -> user_id_num={user_id_num} (type={type(user_id_num)}), product_id_str={product_id_str} -> product_id_num={product_id_num} (type={type(product_id_num)})")
                    debug_count += 1
                
                # Populate dimension tables first
                if user_id_num and user_obj:
                    upsert_user(cursor, user_id_num, user_id_str, user_obj)
                
                if product_id_num and product_obj:
                    upsert_product(cursor, product_id_num, product_id_str, product_obj)
                
                # Now insert fact event
                insert_query = """
                    INSERT INTO fact_events (event_key, session_key, user_id, product_id, device_id, event_type, quantity, event_timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_key) DO NOTHING;
                """
                cursor.execute(insert_query, (
                    event.get("event_id"),
                    event.get("session_id"),
                    user_id_num,
                    product_id_num,
                    device_type,
                    event.get("event_type"),
                    product_obj.get("quantity", 1),
                    event.get("timestamp")
                ))
                
                inserted_count += 1
                if inserted_count % 100 == 0:
                    logger.info(f"✅ Inserted {inserted_count} events into warehouse")
                
            except Exception as e:
                logger.error(f"❌ Error processing event: {e}", exc_info=True)
                continue
        
        cursor.close()
        conn.close()
        logger.info(f"Consumer finished. Total inserted: {inserted_count}")
        
    except Exception as e:
        logger.error(f"❌ Consumer error: {e}", exc_info=True)

if __name__ == '__main__':
    consume_and_write()
