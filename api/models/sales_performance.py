from typing import Optional
from pydantic import BaseModel, ConfigDict


class SalesPerformance(BaseModel):
    sales_rep_id: str
    sales_rep_name: str
    month: Optional[str] = None
    total_opportunities: int = 0
    won_opportunities: int = 0
    lost_opportunities: int = 0
    open_pipeline_value: float = 0.0
    won_pipeline_value: float = 0.0
    win_rate: float = 0.0
    avg_deal_size: float = 0.0

    model_config = ConfigDict(from_attributes=True)