import pandas as pd
from sqlalchemy import create_engine
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define connection credentials from environment variables
DB_USER = os.environ.get("DB_USER", "data_engineer")
DB_PASS = os.environ.get("DB_PASSWORD", "supersecretpassword")
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "ecommerce_warehouse")

# Create the connection engine
connection_string = f"postgresql://{DB_USER}:[REDACTED]@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# SQL Query - select ALL events from fact_events table (no LIMIT)
sql_query = "SELECT * FROM fact_events ORDER BY created_at DESC;"

logger.info(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")

try:
    # Execute the query and load it into Pandas DataFrame
    df = pd.read_sql(sql_query, engine)
    
    logger.info(f"✅ Data successfully loaded into Pandas!")
    logger.info(f"Total rows retrieved: {len(df)}")
    
    # Display basic info
    logger.info("\n--- Data Info ---")
    logger.info(f"Columns: {list(df.columns)}")
    logger.info(f"Shape: {df.shape}")
    
    # Display the first 5 rows
    logger.info("\n--- First 5 Rows ---")
    logger.info(df.head().to_string())
    
    # Ensure output directory exists
    output_dir = "/app/output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Save full dataset to CSV
    csv_file = os.path.join(output_dir, "ecommerce_data_all.csv")
    df.to_csv(csv_file, index=False)
    logger.info(f"\n✅ Full dataset exported to {csv_file}")
    
    # Save summary statistics
    summary_file = os.path.join(output_dir, "ecommerce_data_summary.txt")
    with open(summary_file, "w") as f:
        f.write("=" * 80 + "\n")
        f.write("ECOMMERCE DATA SUMMARY\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Total Events: {len(df)}\n")
        f.write(f"Columns: {', '.join(df.columns)}\n\n")
        f.write("Event Type Breakdown:\n")
        f.write(df['event_type'].value_counts().to_string())
        f.write("\n\nFirst 10 Rows:\n")
        f.write(df.head(10).to_string())
    logger.info(f"✅ Summary exported to {summary_file}")
    
except Exception as e:
    logger.error(f"❌ An error occurred: {e}", exc_info=True)
