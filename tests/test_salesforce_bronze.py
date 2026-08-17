import os
import glob
import pytest
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_BRONZE_PATH = os.getenv("DATA_BRONZE_BASE_PATH", "data/bronze/salesforce")
CORE_OBJECTS = ["account", "contact", "lead", "opportunity", "opportunity_contact_role", "user"]
REQUIRED_METADATA_COLS = ["_ingested_at", "_source_system", "_batch_id"]


@pytest.mark.parametrize("object_name", CORE_OBJECTS)
def test_bronze_parquet_exists_and_valid(object_name):
    """Verifies that Parquet partitions exist and are readable for each object."""
    target_dir = os.path.join(BASE_BRONZE_PATH, object_name)
    parquet_files = glob.glob(os.path.join(target_dir, "*.parquet"))
    
    if not parquet_files:
        pytest.skip(f"No Parquet files generated yet for {object_name}.")

    for file_path in parquet_files:
        df = pd.read_parquet(file_path)
        assert not df.empty, f"Bronze file {file_path} is empty."
        
        # Check required metadata columns
        for col in REQUIRED_METADATA_COLS:
            assert col in df.columns, f"Missing metadata column '{col}' in {file_path}"
            assert df[col].isnull().sum() == 0, f"Null values found in metadata column '{col}'"

        # Check source system identity
        assert (df["_source_system"] == "salesforce").all(), "Invalid _source_system value detected"


def test_user_role_flattening():
    """Verifies that UserRole.Name traversal is flattened to UserRole_Name in User bronze files."""
    user_dir = os.path.join(BASE_BRONZE_PATH, "user")
    parquet_files = glob.glob(os.path.join(user_dir, "*.parquet"))
    
    if not parquet_files:
        pytest.skip("No User Parquet files found to test.")

    for file_path in parquet_files:
        df = pd.read_parquet(file_path)
        assert "UserRole_Name" in df.columns, "SOQL traversal field 'UserRole_Name' missing from User object"