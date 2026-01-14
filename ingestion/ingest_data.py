import pandas as pd
from datetime import datetime
import os

# Directories
RAW_DIR = os.path.join(os.path.dirname(__file__), '../data/raw')
STAGING_DIR = os.path.join(os.path.dirname(__file__), '../data/staging')

# Ensure staging folder exists
os.makedirs(STAGING_DIR, exist_ok=True)

def ingest_file(filename):
    """Read CSV, add metadata, save to staging."""
    filepath = os.path.join(RAW_DIR, filename)
    df = pd.read_csv(filepath)
    
    # Add metadata
    df['ingestion_date'] = datetime.now()
    df['source_file'] = filename
    
    # Save to staging
    staging_path = os.path.join(STAGING_DIR, filename)
    df.to_csv(staging_path, index=False)
    print(f"Ingested {filename} ({len(df)} rows) → staging")

if __name__ == "__main__":
    # Loop over all CSV files in raw folder
    for file in os.listdir(RAW_DIR):
        if file.endswith(".csv"):
            ingest_file(file)
