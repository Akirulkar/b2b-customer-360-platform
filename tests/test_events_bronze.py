from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd

BRONZE_PATH = Path("data/bronze/events")

def verify_bronze_events():
    parquet_files = list(BRONZE_PATH.glob("*.parquet"))
    
    if not parquet_files:
        print("No Parquet files found yet. Make sure producer has sent events.")
        return

    print(f"Found {len(parquet_files)} parquet part files.\n")
    
    # Read dataset
    dataset = pq.ParquetDataset(BRONZE_PATH)
    table = dataset.read()
    df = table.to_pandas()

    print("=== Bronze Events Schema ===")
    print(table.schema)
    
    print("\n=== Record Count ===")
    print(f"Total Bronze Events Ingested: {len(df)}")
    
    print("\n=== Event Type Breakdown ===")
    print(df["event_type"].value_counts())

    print("\n=== Sample Records (with Bronze Metadata) ===")
    cols_to_display = ["event_id", "event_type", "timestamp", "_kafka_offset", "_kafka_partition", "_ingested_at", "_source_system"]
    print(df[cols_to_display].head(5).to_string(index=False))

if __name__ == "__main__":
    verify_bronze_events()