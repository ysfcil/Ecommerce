import json
import logging
import os
from kafka import KafkaConsumer
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_postgres():
    db_host = os.environ.get("DB_HOST", "postgres")
    db_port = os.environ.get("DB_PORT", "5432")
    db_user = os.environ.get("DB_USER", "data_engineer")
    db_password = os.environ.get("DB_PASSWORD", "supersecretpassword")
    db_name = os.environ.get("DB_NAME", "ecommerce_warehouse")
    
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    conn.autocommit = True
    return conn

def main():
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    logger.info(f"Connecting to Kafka at {kafka_bootstrap}...")
    consumer = KafkaConsumer(
        'ecommerce_clickstream',
        bootstrap_servers=[kafka_bootstrap],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='warehouse-consumer-group'
    )
    
    logger.info("Connected to Kafka. Connecting to PostgreSQL...")
    conn = connect_postgres()
    logger.info("Connected to PostgreSQL warehouse")
    
    cursor = conn.cursor()
    event_count = 0
    
    try:
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
                    event.get("timestamp"),
                    event.get("user"),
                    event.get("product")
                ))
                event_count += 1
                
                if event_count % 100 == 0:
                    logger.info(f"Processed {event_count} events")
                    
            except Exception as e:
                logger.error(f"Error processing event: {e}")
    
    except KeyboardInterrupt:
        logger.info(f"Shutting down. Total events processed: {event_count}")
    finally:
        cursor.close()
        conn.close()
        consumer.close()

if __name__ == '__main__':
    main()
