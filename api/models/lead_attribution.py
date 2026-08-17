from typing import Optional
from pydantic import BaseModel, ConfigDict


class LeadAttribution(BaseModel):
    lead_id: str
    lead_source: Optional[str] = None
    is_converted: bool
    account_id: Optional[str] = None
    opportunity_id: Optional[str] = None
    opportunity_amount: Optional[float] = 0.0
    is_won: bool = False

    model_config = ConfigDict(from_attributes=True)