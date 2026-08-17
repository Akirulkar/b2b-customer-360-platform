from pathlib import Path
import pyarrow.parquet as pq
import pytest

BRONZE_DIR = Path("data/bronze/events")
CHECKPOINT_DIR = Path("data/checkpoints/events_to_bronze")

def test_bronze_files_and_checkpoints_exist():
    assert BRONZE_DIR.exists(), "Bronze events directory does not exist"
    assert CHECKPOINT_DIR.exists(), "Checkpoint directory does not exist"
    
    parquet_files = list(BRONZE_DIR.glob("*.parquet"))
    assert len(parquet_files) > 0, "No parquet files written to Bronze"

def test_bronze_schema_and_metadata_contract():
    dataset = pq.ParquetDataset(BRONZE_DIR)
    table = dataset.read()
    df = table.to_pandas()

    expected_cols = {
        "event_id",
        "event_type",
        "timestamp",
        "identity_resolution",
        "attributes",
        "_kafka_offset",
        "_kafka_partition",
        "_ingested_at",
        "_source_system",
    }
    assert expected_cols.issubset(set(df.columns)), f"Missing columns in Bronze: {expected_cols - set(df.columns)}"

    # Metadata integrity
    assert (df["_source_system"] == "digital_events").all(), "_source_system must be 'digital_events'"
    assert df["_ingested_at"].notnull().all(), "_ingested_at must not be null"
    assert df["_kafka_offset"].notnull().all(), "_kafka_offset must not be null"
    assert df["_kafka_partition"].notnull().all(), "_kafka_partition must not be null"

    # Contract data integrity
    assert df["event_id"].notnull().all(), "event_id cannot have nulls"
    assert (df["event_id"].str.startswith("evt_")).all(), "event_id format must start with 'evt_'"