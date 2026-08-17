import math
from pathlib import Path
from typing import Any, Dict, List, Optional
import pyarrow.dataset as ds
from fastapi import HTTPException, status
from api.config import settings


class GoldDataService:
    def _clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert float('nan') and inf values to Python None for Pydantic."""
        cleaned = {}
        for k, v in record.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                cleaned[k] = None
            else:
                cleaned[k] = v
        return cleaned

    def _read_parquet_records(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Gold Mart storage unavailable or missing at {path.name}",
            )
        try:
            dataset = ds.dataset(str(path), format="parquet")
            table = dataset.to_table()
            df = table.to_pandas()
            raw_records = df.to_dict(orient="records")
            return [self._clean_record(r) for r in raw_records]
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to query {path.name}: {str(e)}",
            )

    def get_customers(
        self,
        account_id: Optional[str] = None,
        industry: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        records = self._read_parquet_records(settings.CUSTOMER_360_PATH)
        if account_id:
            records = [r for r in records if r.get("account_id") == account_id]
        if industry:
            records = [r for r in records if (r.get("industry") or "").lower() == industry.lower()]
        return records[offset : offset + limit]

    def get_intent_engagement(
        self, account_id: Optional[str] = None, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        records = self._read_parquet_records(settings.INTENT_ENGAGEMENT_PATH)
        if account_id:
            records = [r for r in records if r.get("account_id") == account_id]
        return records[offset : offset + limit]

    def get_sales_prioritization(
        self,
        priority_band: Optional[str] = None,
        sales_rep_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        records = self._read_parquet_records(settings.SALES_PRIORITIZATION_PATH)
        if priority_band:
            records = [r for r in records if (r.get("priority_band") or "").lower() == priority_band.lower()]
        if sales_rep_id:
            records = [r for r in records if r.get("sales_rep_id") == sales_rep_id]
        records.sort(key=lambda x: (x.get("priority_score") or 0.0), reverse=True)
        return records[offset : offset + limit]

    def get_sales_performance(
        self,
        sales_rep_id: Optional[str] = None,
        month: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        records = self._read_parquet_records(settings.SALES_PERFORMANCE_PATH)
        if sales_rep_id:
            records = [r for r in records if r.get("sales_rep_id") == sales_rep_id]
        if month:
            records = [r for r in records if str(r.get("month")) == month]
        return records[offset : offset + limit]

    def get_lead_attribution(
        self,
        lead_source: Optional[str] = None,
        is_converted: Optional[bool] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        records = self._read_parquet_records(settings.LEAD_ATTRIBUTION_PATH)
        if lead_source:
            records = [r for r in records if (r.get("lead_source") or "").lower() == lead_source.lower()]
        if is_converted is not None:
            records = [r for r in records if bool(r.get("is_converted")) is is_converted]
        return records[offset : offset + limit]


gold_service = GoldDataService()