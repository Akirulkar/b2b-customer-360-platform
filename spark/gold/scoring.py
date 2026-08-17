"""spark/gold/scoring.py
Business rules for Intent Scoring, Recency Decay, and Sales Prioritization.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

# Event interaction weights mapping
INTENT_WEIGHTS = {
    "website_visit": 1,
    "product_page_view": 3,
    "documentation_view": 3,
    "brochure_download": 5,
    "pricing_page_view": 8,
    "demo_request": 15,
}

def get_event_weight_expr(col_name: str = "event_type") -> Column:
    """Translates event types into integer intent weights."""
    mapping_expr = F.when(F.col(col_name) == "website_visit", 1)\
        .when(F.col(col_name) == "product_page_view", 3)\
        .when(F.col(col_name) == "documentation_view", 3)\
        .when(F.col(col_name) == "brochure_download", 5)\
        .when(F.col(col_name) == "pricing_page_view", 8)\
        .when(F.col(col_name) == "demo_request", 15)\
        .otherwise(0)
    return mapping_expr

def calculate_recency_factor(days_col: str) -> Column:
    """Applies a continuous exponential/step decay factor based on days since activity.
    
    - 0-7 days: 1.0 (100%)
    - 8-14 days: 0.7 (70%)
    - 15-30 days: 0.4 (40%)
    - >30 days: 0.1 (10%)
    """
    return (
        F.when(F.col(days_col) <= 7, 1.0)
        .when(F.col(days_col) <= 14, 0.7)
        .when(F.col(days_col) <= 30, 0.4)
        .otherwise(0.1)
    )

def compute_priority_score(
    intent_norm_col: str,
    pipeline_norm_col: str,
    recency_norm_col: str,
    has_open_opp_col: str
) -> Column:
    """Calculates composite priority score (0-100 scale).
    
    Weights:
    - Intent Score: 40%
    - Pipeline Value: 30%
    - Recency Factor: 20%
    - Active Opportunity State: 10%
    """
    return (
        (F.col(intent_norm_col) * 0.40) +
        (F.col(pipeline_norm_col) * 0.30) +
        (F.col(recency_norm_col) * 0.20) +
        (F.when(F.col(has_open_opp_col), 100.0).otherwise(0.0) * 0.10)
    )