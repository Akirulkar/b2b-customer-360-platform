from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from api.dependencies import get_gold_service
from api.models.lead_attribution import LeadAttribution
from api.services.gold_service import GoldDataService

router = APIRouter(prefix="/lead-attribution", tags=["Lead Attribution"])


@router.get("", response_model=List[LeadAttribution])
def list_lead_attribution(
    lead_source: Optional[str] = Query(None, description="e.g. Web, Referral, Partner"),
    is_converted: Optional[bool] = Query(None, description="Filter by conversion status"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: GoldDataService = Depends(get_gold_service),
):
    return service.get_lead_attribution(
        lead_source=lead_source,
        is_converted=is_converted,
        limit=limit,
        offset=offset,
    )