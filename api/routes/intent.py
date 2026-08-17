from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from api.dependencies import get_gold_service
from api.models.intent import IntentEngagement
from api.services.gold_service import GoldDataService

router = APIRouter(prefix="/intent", tags=["Intent & Engagement"])


@router.get("", response_model=List[IntentEngagement])
def list_intent_engagement(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: GoldDataService = Depends(get_gold_service),
):
    return service.get_intent_engagement(limit=limit, offset=offset)


@router.get("/{account_id}", response_model=IntentEngagement)
def get_intent_by_account_id(
    account_id: str,
    service: GoldDataService = Depends(get_gold_service),
):
    results = service.get_intent_engagement(account_id=account_id, limit=1)
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No intent records found for account ID '{account_id}'",
        )
    return results[0]