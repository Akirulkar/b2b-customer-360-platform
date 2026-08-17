import os
import logging
from typing import Dict, Any
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("BronzeWriter")


class BronzeWriter:
    """Persists extracted raw records into Bronze Parquet storage."""

    def __init__(self, base_path: str = None):
        self.base_path = base_path or os.getenv("DATA_BRONZE_BASE_PATH", "data/bronze/salesforce")

    def write(self, extraction_payload: Dict[str, Any]) -> str:
        """Writes batch extraction records into an append-only Parquet partition."""
        object_name = extraction_payload["object_name"]
        records = extraction_payload["records"]
        batch_id = extraction_payload["batch_id"]

        if not records:
            logger.info(f"No records to write for {object_name}.")
            return ""

        target_dir = os.path.join(self.base_path, object_name)
        os.makedirs(target_dir, exist_ok=True)

        file_path = os.path.join(target_dir, f"batch_{batch_id}.parquet")
        
        # Convert records to Pandas DataFrame and persist to Parquet
        df = pd.DataFrame(records)
        df.to_parquet(file_path, index=False, engine="pyarrow")

        logger.info(f"Successfully wrote {len(records)} records to {file_path}")
        return file_path