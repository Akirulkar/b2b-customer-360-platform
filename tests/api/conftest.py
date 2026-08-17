import pytest
import pandas as pd
from fastapi.testclient import TestClient
from api.config import settings
from api.main import app


@pytest.fixture(scope="session")
def client():
    return TestClient(app)


@pytest.fixture(scope="session", autouse=True)
def setup_mock_gold_parquet(tmp_path_factory):
    """Creates temporary parquet files simulating the Gold marts."""
    gold_dir = tmp_path_factory.mktemp("gold")
    
    # 1. Customer 360
    cust_df = pd.DataFrame([{
        "account_id": "acc_001",
        "account_name": "Acme Corp",
        "industry": "Technology",
        "employee_count": 500,
        "annual_revenue": 50000000.0,
        "contact_count": 3,
        "open_opportunity_count": 2,
        "open_pipeline_value": 75000.0,
        "won_opportunity_count": 1,
        "won_pipeline_value": 120000.0,
        "lost_opportunity_count": 0,
    }])
    cust_path = gold_dir / "customer_360"
    cust_path.mkdir()
    cust_df.to_parquet(cust_path / "part-0.parquet")
    settings.CUSTOMER_360_PATH = cust_path

    # 2. Intent
    intent_df = pd.DataFrame([{
        "account_id": "acc_001",
        "window_start_date": "2026-08-01",
        "window_end_date": "2026-08-18",
        "website_visits": 5,
        "product_views": 3,
        "pricing_views": 2,
        "documentation_views": 1,
        "demo_requests": 1,
        "intent_score": 45.0,
    }])
    intent_path = gold_dir / "intent_engagement"
    intent_path.mkdir()
    intent_df.to_parquet(intent_path / "part-0.parquet")
    settings.INTENT_ENGAGEMENT_PATH = intent_path

    # 3. Prioritization
    prio_df = pd.DataFrame([{
        "account_id": "acc_001",
        "account_name": "Acme Corp",
        "sales_rep_id": "rep_101",
        "sales_rep_name": "John Doe",
        "intent_score": 45.0,
        "open_pipeline_value": 75000.0,
        "priority_score": 88.5,
        "priority_band": "Tier 1",
    }])
    prio_path = gold_dir / "sales_prioritization"
    prio_path.mkdir()
    prio_df.to_parquet(prio_path / "part-0.parquet")
    settings.SALES_PRIORITIZATION_PATH = prio_path

    # 4. Sales Performance
    perf_df = pd.DataFrame([{
        "sales_rep_id": "rep_101",
        "sales_rep_name": "John Doe",
        "month": "2026-08",
        "total_opportunities": 5,
        "won_opportunities": 3,
        "lost_opportunities": 1,
        "open_pipeline_value": 75000.0,
        "won_pipeline_value": 250000.0,
        "win_rate": 0.75,
        "avg_deal_size": 83333.33,
    }])
    perf_path = gold_dir / "sales_performance"
    perf_path.mkdir()
    perf_df.to_parquet(perf_path / "part-0.parquet")
    settings.SALES_PERFORMANCE_PATH = perf_path

    # 5. Lead Attribution
    lead_df = pd.DataFrame([{
        "lead_id": "lead_99",
        "lead_source": "Web",
        "is_converted": True,
        "account_id": "acc_001",
        "opportunity_id": "opp_501",
        "opportunity_amount": 50000.0,
        "is_won": True,
    }])
    lead_path = gold_dir / "lead_attribution"
    lead_path.mkdir()
    lead_df.to_parquet(lead_path / "part-0.parquet")
    settings.LEAD_ATTRIBUTION_PATH = lead_path