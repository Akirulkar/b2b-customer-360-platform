from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from api.dependencies import get_gold_service
from api.models.prioritization import SalesPrioritization
from api.services.gold_service import GoldDataService

router = APIRouter(prefix="/prioritization", tags=["Sales Prioritization"])


@router.get("", response_model=List[SalesPrioritization])
def list_sales_prioritization(
    priority_band: Optional[str] = Query(None, description="e.g. Tier 1, Tier 2, Tier 3"),
    sales_rep_id: Optional[str] = Query(None, description="Filter by assigned sales rep ID"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: GoldDataService = Depends(get_gold_service),
):
    return service.get_sales_prioritization(
        priority_band=priority_band,
        sales_rep_id=sales_rep_id,
        limit=limit,
        offset=offset,
    )