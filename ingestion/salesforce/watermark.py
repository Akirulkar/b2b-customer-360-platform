import os
import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from dotenv import load_dotenv
from ingestion.utils.logger import logger
load_dotenv()


class WatermarkManager:
    """Handles persistent watermark state management for incremental extractions."""

    def __init__(self, base_path: Optional[str] = None):
        self.base_path = base_path or os.getenv(
            "METADATA_WATERMARK_PATH", "metadata_raw/watermarks/salesforce"
        )
        os.makedirs(self.base_path, exist_ok=True)

    def _get_watermark_file(self, object_name: str) -> str:
        return os.path.join(self.base_path, f"{object_name}.json")

    def get_watermark(self, object_name: str) -> Optional[str]:
        """Reads the last successful extraction watermark timestamp for an object."""
        file_path = self._get_watermark_file(object_name)
        if not os.path.exists(file_path):
            logger.info(f"No watermark found for '{object_name}'. Triggering FULL load.")
            return None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data: Dict[str, Any] = json.load(f)
                watermark = data.get("last_successful_watermark")
                logger.info(f"Loaded watermark for '{object_name}': {watermark}")
                return watermark
        except Exception as e:
            logger.error(f"Error reading watermark for '{object_name}': {str(e)}")
            return None

    def update_watermark(
        self, object_name: str, watermark: str, batch_id: str, record_count: int
    ) -> None:
        """
        Updates the watermark file atomically after a confirmed Bronze layer write.
        """
        file_path = self._get_watermark_file(object_name)
        state_payload = {
            "object_name": object_name,
            "last_successful_watermark": watermark,
            "last_batch_id": batch_id,
            "record_count": record_count,
            "status": "SUCCESS",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }

        # Write to a temporary file first, then replace for atomic persistence
        temp_file = f"{file_path}.tmp"
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(state_payload, f, indent=2)
            os.replace(temp_file, file_path)
            logger.info(f"Watermark updated for '{object_name}' -> {watermark}")
        except Exception as e:
            logger.error(f"Failed to persist watermark for '{object_name}': {str(e)}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise