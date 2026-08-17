import uuid
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from ingestion.salesforce.client import SalesforceClient
from ingestion.salesforce.queries import build_soql
from ingestion.utils.logger import logger


class SalesforceExtractor:
    """Extracts raw Salesforce objects and enriches them with Bronze metadata."""

    def __init__(self, client: Optional[SalesforceClient] = None):
        self.client = client or SalesforceClient()

    def extract(self, object_name: str, watermark: Optional[str] = None) -> Dict[str, Any]:
        """
        Executes full or incremental extraction for an object.
        Returns a dictionary with batch metadata and records.
        """
        batch_id = str(uuid.uuid4())
        ingested_at = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Starting extraction for object='{object_name}' (Batch: {batch_id})")
        soql = build_soql(object_name, watermark_timestamp=watermark)
        
        raw_records = self.client.execute_query(soql)
        
        # Flatten and inject Bronze operational metadata
        enriched_records = []
        max_modstamp = watermark

        for record in raw_records:
            # Flatten UserRole.Name traversal if present
            if "UserRole" in record:
                user_role = record.pop("UserRole")
                record["UserRole_Name"] = user_role.get("Name") if isinstance(user_role, dict) else None

            # Inject Bronze Metadata
            record["_ingested_at"] = ingested_at
            record["_source_system"] = "salesforce"
            record["_batch_id"] = batch_id

            # Track latest SystemModstamp for watermark
            rec_modstamp = record.get("SystemModstamp")
            if rec_modstamp:
                if max_modstamp is None or rec_modstamp > max_modstamp:
                    max_modstamp = rec_modstamp

            enriched_records.append(record)

        return {
            "object_name": object_name,
            "batch_id": batch_id,
            "record_count": len(enriched_records),
            "max_watermark": max_modstamp,
            "ingested_at": ingested_at,
            "records": enriched_records
        }