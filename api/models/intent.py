from typing import Optional
from datetime import date
from pydantic import BaseModel, ConfigDict


class IntentEngagement(BaseModel):
    account_id: str
    window_start_date: Optional[date] = None
    window_end_date: Optional[date] = None
    website_visits: int = 0
    product_views: int = 0
    pricing_views: int = 0
    documentation_views: int = 0
    demo_requests: int = 0
    intent_score: float = 0.0

    model_config = ConfigDict(from_attributes=True)