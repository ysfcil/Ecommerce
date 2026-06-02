#!/usr/bin/env python3
"""One-shot reader that queries all events and exports to CSV files."""
import sys
sys.path.insert(0, '/opt/venv/lib/python3.11/site-packages')

import pandas as pd
from sqlalchemy import create_engine
import os

# DB connection
db_url = f"postgresql://data_engineer:supersecretpassword@postgres:5432/ecommerce_warehouse"
engine = create_engine(db_url)

# Query ALL rows
query = "SELECT * FROM fact_events ORDER BY created_at DESC;"
df = pd.read_sql(query, engine)

print(f"Total rows: {len(df)}")
print(f"Shape: {df.shape}")
print(f"\nFirst 5 rows:\n{df.head()}")

# Write CSVs
output_dir = "/app/output"
os.makedirs(output_dir, exist_ok=True)

csv_file = os.path.join(output_dir, "ecommerce_data_all.csv")
df.to_csv(csv_file, index=False)
print(f"\n✅ Exported {len(df)} rows to {csv_file}")

summary_file = os.path.join(output_dir, "ecommerce_data_summary.txt")
with open(summary_file, "w") as f:
    f.write("="*80 + "\n")
    f.write("ECOMMERCE DATA SUMMARY\n")
    f.write("="*80 + "\n\n")
    f.write(f"Total Events: {len(df)}\n")
    f.write(f"Columns: {', '.join(df.columns)}\n\n")
    f.write("Event Type Breakdown:\n")
    f.write(df['event_type'].value_counts().to_string())
    f.write("\n\nFirst 20 Rows:\n")
    f.write(df.head(20).to_string())

print(f"✅ Exported summary to {summary_file}")
engine.dispose()
