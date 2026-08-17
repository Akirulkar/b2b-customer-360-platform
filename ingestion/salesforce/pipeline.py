import logging
from typing import List, Optional
from ingestion.salesforce.client import SalesforceClient
from ingestion.salesforce.extractor import SalesforceExtractor
from ingestion.salesforce.watermark import WatermarkManager
from spark.bronze.salesforce.write_bronze import BronzeWriter
from ingestion.utils.logger import logger

OBJECTS_TO_INGEST = [
    "account",
    "contact",
    "lead",
    "opportunity",
    "opportunity_contact_role",
    "user"
]


class SalesforceBronzePipeline:
    """Orchestrates extraction from Salesforce and persistence into Bronze storage."""

    def __init__(self):
        self.client = SalesforceClient()
        self.extractor = SalesforceExtractor(client=self.client)
        self.writer = BronzeWriter()
        self.watermark_mgr = WatermarkManager()

    def run_object(self, object_name: str, force_full_load: bool = False) -> None:
        """Runs the extraction and ingestion pipeline for a single Salesforce object."""
        logger.info(f"--- Starting Ingestion: {object_name.upper()} ---")
        
        # 1. Retrieve watermark state
        watermark = None if force_full_load else self.watermark_mgr.get_watermark(object_name)

        # 2. Extract records
        payload = self.extractor.extract(object_name=object_name, watermark=watermark)
        record_count = payload["record_count"]

        if record_count == 0:
            logger.info(f"No new or modified records found for '{object_name}'. Skipping write.")
            return

        # 3. Write to Bronze Storage (Parquet)
        output_file = self.writer.write(payload)
        logger.info(f"Landed {record_count} records in Bronze file: {output_file}")

        # 4. Atomic Watermark Update (Only upon verified extraction write)
        if payload["max_watermark"]:
            self.watermark_mgr.update_watermark(
                object_name=object_name,
                watermark=payload["max_watermark"],
                batch_id=payload["batch_id"],
                record_count=record_count
            )

        logger.info(f"--- Completed Ingestion: {object_name.upper()} ---")

    def run_all(self, objects: Optional[List[str]] = None, force_full_load: bool = False) -> None:
        """Executes sequential batch ingestion for all configured Salesforce objects."""
        target_objects = objects or OBJECTS_TO_INGEST
        logger.info(f"Starting Bronze Ingestion Run for: {target_objects}")

        for obj in target_objects:
            try:
                self.run_object(object_name=obj, force_full_load=force_full_load)
            except Exception as e:
                logger.error(f"Ingestion pipeline failed for object '{obj}': {str(e)}", exc_info=True)
                # Continue processing other objects or raise depending on operational policy
                continue

        logger.info("Bronze Batch Ingestion Run Finished.")


if __name__ == "__main__":
    pipeline = SalesforceBronzePipeline()
    # Run full load on initial execution
    pipeline.run_all(force_full_load=False)