from typing import Optional
from pydantic import BaseModel, ConfigDict


class SalesPrioritization(BaseModel):
    account_id: str
    account_name: str
    sales_rep_id: Optional[str] = None
    sales_rep_name: Optional[str] = None
    intent_score: float = 0.0
    open_pipeline_value: float = 0.0
    priority_score: float
    priority_band: str

    model_config = ConfigDict(from_attributes=True)