from typing import Optional
from pydantic import BaseModel, ConfigDict


class Customer360(BaseModel):
    account_id: str
    account_name: Optional[str] = "Unknown"
    industry: Optional[str] = None
    employee_count: Optional[int] = None
    annual_revenue: Optional[float] = None
    contact_count: Optional[int] = 0
    open_opportunity_count: Optional[int] = 0
    open_pipeline_value: Optional[float] = 0.0
    won_opportunity_count: Optional[int] = 0
    won_pipeline_value: Optional[float] = 0.0
    lost_opportunity_count: Optional[int] = 0

    model_config = ConfigDict(from_attributes=True)