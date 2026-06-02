#!/usr/bin/env python3
"""
Simple Kafka consumer that writes events to PostgreSQL.
Replaces Apache Beam for simpler, faster data ingestion.
"""
import json
import os
import logging
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database config
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "data_engineer")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "supersecretpassword")
DB_NAME = os.environ.get("DB_NAME", "ecommerce_warehouse")

# Kafka config
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

def consume_and_write():
    """Consume Kafka messages and write to PostgreSQL"""
    try:
        # Connect to Kafka
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
        consumer = KafkaConsumer(
            'ecommerce_clickstream',
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset='earliest',
            group_id='simple-consumer',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        logger.info("✅ Connected to Kafka")
        
        # Connect to database
        conn = connect_db()
        cursor = conn.cursor()
        
        inserted_count = 0
        for message in consumer:
            try:
                event = message.value
                
                insert_query = """
                    INSERT INTO fact_events (event_key, session_key, event_type, event_timestamp)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (event_key) DO NOTHING;
                """
                cursor.execute(insert_query, (
                    event.get("event_id"),
                    event.get("session_id"),
                    event.get("event_type"),
                    event.get("timestamp")
                ))
                
                inserted_count += 1
                if inserted_count % 100 == 0:
                    logger.info(f"✅ Inserted {inserted_count} events into warehouse")
                
            except Exception as e:
                logger.error(f"❌ Error processing event: {e}")
                continue
        
        cursor.close()
        conn.close()
        logger.info(f"Consumer finished. Total inserted: {inserted_count}")
        
    except Exception as e:
        logger.error(f"❌ Consumer error: {e}", exc_info=True)

if __name__ == '__main__':
    consume_and_write()
