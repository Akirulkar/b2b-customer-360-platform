from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from api.dependencies import get_gold_service
from api.models.customer import Customer360
from api.services.gold_service import GoldDataService

router = APIRouter(prefix="/customers", tags=["Customer 360"])


@router.get("", response_model=List[Customer360])
def list_customers(
    industry: Optional[str] = Query(None, description="Filter by account industry"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: GoldDataService = Depends(get_gold_service),
):
    return service.get_customers(industry=industry, limit=limit, offset=offset)


@router.get("/{account_id}", response_model=Customer360)
def get_customer_by_id(
    account_id: str,
    service: GoldDataService = Depends(get_gold_service),
):
    results = service.get_customers(account_id=account_id, limit=1)
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Account with ID '{account_id}' not found",
        )
    return results[0]