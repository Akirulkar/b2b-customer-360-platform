"""tests/gold/test_sales_prioritization.py
Test suite for gold.sales_prioritization mart.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import date, datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType
)
from spark.gold.sales_prioritization import build_sales_prioritization

def test_sales_prioritization_scoring_and_tiers(spark):
    c360_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), True),
        StructField("sales_rep_id", StringType(), True),
        StructField("sales_rep_name", StringType(), True),
        StructField("open_pipeline_value", DoubleType(), True),
        StructField("open_opportunity_count", IntegerType(), True),
        StructField("last_activity_at", TimestampType(), True)
    ])

    intent_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("event_date", DateType(), False),
        StructField("intent_score", IntegerType(), True),
        StructField("engagement_score", IntegerType(), True)
    ])

    c360_data = [
        # acc_high: Big pipeline, recent intent, active opp -> High Priority (Tier 1)
        ("acc_high", "High Intent Co", "srep_1", "Alice", 2000000.0, 2, datetime(2026, 8, 17, 10, 0, 0)),
        # acc_cold: Zero pipeline, no recent activity -> Low Priority (Tier 3)
        ("acc_cold", "Dormant Co", "srep_2", "Bob", 0.0, 0, datetime(2026, 5, 1, 10, 0, 0))
    ]

    intent_data = [
        ("acc_high", date(2026, 8, 17), 45, 5),
        ("acc_cold", date(2026, 5, 1), 10, 2)  # Older than 14d window
    ]

    c360_df = spark.createDataFrame(c360_data, c360_schema)
    intent_df = spark.createDataFrame(intent_data, intent_schema)

    result_df = build_sales_prioritization(c360_df, intent_df, as_of_date="2026-08-18")
    rows = {r["account_id"]: r for r in result_df.collect()}

    # Verify count and grain
    assert result_df.count() == 2

    # High Intent Account assertions
    acc_high = rows["acc_high"]
    assert acc_high["intent_score"] == 45
    assert acc_high["days_since_last_activity"] == 1
    assert acc_high["priority_score"] >= 70.0
    assert acc_high["priority_band"] == "Tier 1"

    # Cold Account assertions
    acc_cold = rows["acc_cold"]
    assert acc_cold["intent_score"] == 0  # 14-day window filters out old activity
    assert acc_cold["days_since_last_activity"] > 30
    assert acc_cold["priority_score"] < 40.0
    assert acc_cold["priority_band"] == "Tier 3"