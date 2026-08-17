from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PROJECT_NAME: str = "B2B Customer 360 & Analytics API"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"
    
    # Path to Gold Data Marts
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    GOLD_DATA_PATH: Path = BASE_DIR / "data" / "gold"
    
    # Mart Specific Paths
    CUSTOMER_360_PATH: Path = GOLD_DATA_PATH / "customer_360"
    INTENT_ENGAGEMENT_PATH: Path = GOLD_DATA_PATH / "intent_engagement"
    SALES_PRIORITIZATION_PATH: Path = GOLD_DATA_PATH / "sales_prioritization"
    SALES_PERFORMANCE_PATH: Path = GOLD_DATA_PATH / "sales_performance"
    LEAD_ATTRIBUTION_PATH: Path = GOLD_DATA_PATH / "lead_attribution"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()