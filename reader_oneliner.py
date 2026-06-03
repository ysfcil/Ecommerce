#!/usr/bin/env python3
"""One-shot reader that queries all events and exports to CSV files."""
import sys
sys.path.insert(0, '/opt/venv/lib/python3.11/site-packages')

import pandas as pd
from sqlalchemy import create_engine
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DB connection from environment
db_user = os.environ.get("DB_USER", "data_engineer")
db_pass = os.environ.get("DB_PASSWORD", "supersecretpassword")
db_host = os.environ.get("DB_HOST", "postgres")
db_port = os.environ.get("DB_PORT", "5432")
db_name = os.environ.get("DB_NAME", "ecommerce_warehouse")

db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

logger.info(f"Connecting to PostgreSQL at {db_host}:{db_port}/{db_name}...")

try:
    # Query ALL rows
    query = "SELECT * FROM fact_events ORDER BY created_at DESC;"
    df = pd.read_sql(query, engine)

    logger.info(f"✅ Data successfully loaded into Pandas!")
    logger.info(f"Total rows: {len(df)}")
    logger.info(f"Shape: {df.shape}")
    logger.info(f"\nFirst 5 rows:\n{df.head()}")

    # Write CSVs
    output_dir = "/app/output"
    os.makedirs(output_dir, exist_ok=True)

    csv_file = os.path.join(output_dir, "ecommerce_data_all.csv")
    df.to_csv(csv_file, index=False)
    logger.info(f"✅ Exported {len(df)} rows to {csv_file}")

    summary_file = os.path.join(output_dir, "ecommerce_data_summary.txt")
    with open(summary_file, "w") as f:
        f.write("="*80 + "\n")
        f.write("ECOMMERCE DATA SUMMARY\n")
        f.write("="*80 + "\n\n")
        f.write(f"Total Events: {len(df)}\n")
        f.write(f"Columns: {', '.join(df.columns)}\n\n")
        if len(df) > 0 and 'event_type' in df.columns:
            f.write("Event Type Breakdown:\n")
            f.write(df['event_type'].value_counts().to_string())
        f.write("\n\nFirst 20 Rows:\n")
        f.write(df.head(20).to_string())

    logger.info(f"✅ Exported summary to {summary_file}")
except Exception as e:
    logger.error(f"❌ Error: {e}", exc_info=True)
finally:
    engine.dispose()
