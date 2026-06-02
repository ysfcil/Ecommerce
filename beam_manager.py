import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
import json
import psycopg2
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom DoFn to write data to PostgreSQL
class WriteToPostgres(beam.DoFn):
    def setup(self):
        # Initialize connection when the worker starts
        db_host = os.environ.get("DB_HOST", "postgres")
        db_port = os.environ.get("DB_PORT", "5432")
        db_user = os.environ.get("DB_USER", "data_engineer")
        db_password = os.environ.get("DB_PASSWORD", "supersecretpassword")
        db_name = os.environ.get("DB_NAME", "ecommerce_warehouse")
        
        try:
            self.conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port
            )
            self.conn.autocommit = True
            logger.info(f"Connected to PostgreSQL at {db_host}:{db_port}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def process(self, element):
        # The element comes in as a byte string from Kafka, so we decode and parse it
        try:
            # Kafka records are tuples of (key, value)
            raw_data = element[1].decode('utf-8')
            event = json.loads(raw_data)
            
            # Insert into fact_events table
            cursor = self.conn.cursor()
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
            cursor.close()
            logger.info(f"Inserted event {event.get('event_id')} into warehouse")
            
        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)

    def teardown(self):
        # Close connection when worker stops
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")

def run():
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    options = PipelineOptions([
        '--runner=DirectRunner',
        '--direct_num_workers=1',
        '--streaming',
        '--save_main_session'
    ])
    
    options.view_as(StandardOptions).streaming = True
    
    logger.info(f"Starting Apache Beam Stream Processor (Kafka: {kafka_bootstrap})...")
    
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from Kafka' >> ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': kafka_bootstrap,
                    'auto.offset.reset': 'earliest',
                    'group.id': 'ecommerce-beam-consumer'
                },
                topics=['ecommerce_clickstream'],
                with_metadata=False
            )
            | 'Write to PostgreSQL' >> beam.ParDo(WriteToPostgres())
        )

if __name__ == '__main__':
    run()
