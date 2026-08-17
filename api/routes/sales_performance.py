from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from api.dependencies import get_gold_service
from api.models.sales_performance import SalesPerformance
from api.services.gold_service import GoldDataService

router = APIRouter(prefix="/sales-performance", tags=["Sales Performance"])


@router.get("", response_model=List[SalesPerformance])
def list_sales_performance(
    sales_rep_id: Optional[str] = Query(None, description="Filter by sales rep ID"),
    month: Optional[str] = Query(None, description="Filter by month (YYYY-MM)"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: GoldDataService = Depends(get_gold_service),
):
    return service.get_sales_performance(
        sales_rep_id=sales_rep_id,
        month=month,
        limit=limit,
        offset=offset,
    )