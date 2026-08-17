"""tests/gold/test_lead_attribution.py
Test suite for gold.lead_attribution mart.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import date, datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, DateType
)
from spark.gold.lead_attribution import build_lead_attribution

def test_lead_attribution_metrics_and_lifecycle(spark):
    lead_schema = StructType([
        StructField("lead_id", StringType(), False),
        StructField("lead_source", StringType(), True),
        StructField("status", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("is_converted", BooleanType(), False),
        StructField("converted_account_id", StringType(), True),
        StructField("converted_contact_id", StringType(), True),
        StructField("converted_opportunity_id", StringType(), True)
    ])

    opp_schema = StructType([
        StructField("opportunity_id", StringType(), False),
        StructField("amount", DoubleType(), True),
        StructField("is_won", BooleanType(), True),
        StructField("close_date", DateType(), True)
    ])

    lead_data = [
        # Converted Lead with Won Opportunity: created 2026-08-01, converted 2026-08-11 (10 days)
        (
            "led_1", "Organic Search", "Closed - Converted", "Hot",
            datetime(2026, 8, 1, 9, 0, 0), datetime(2026, 8, 11, 14, 0, 0),
            True, "acc_101", "cnt_201", "opp_301"
        ),
        # Unconverted Lead
        (
            "led_2", "Paid Ads", "Open - Not Contacted", "Cold",
            datetime(2026, 8, 10, 10, 0, 0), datetime(2026, 8, 12, 10, 0, 0),
            False, None, None, None
        )
    ]

    opp_data = [
        ("opp_301", 125000.0, True, date(2026, 8, 15))
    ]

    leads_df = spark.createDataFrame(lead_data, lead_schema)
    opps_df = spark.createDataFrame(opp_data, opp_schema)

    result_df = build_lead_attribution(leads_df, opps_df)
    rows = {r["lead_id"]: r for r in result_df.collect()}

    # Check row count
    assert result_df.count() == 2

    # Converted lead assertions
    led_1 = rows["led_1"]
    assert led_1["is_converted"] is True
    assert str(led_1["conversion_date"]) == "2026-08-11"
    assert led_1["days_to_conversion"] == 10
    assert led_1["converted_opportunity_amount"] == 125000.0
    assert led_1["is_won_deal"] is True

    # Unconverted lead assertions
    led_2 = rows["led_2"]
    assert led_2["is_converted"] is False
    assert led_2["conversion_date"] is None
    assert led_2["days_to_conversion"] is None
    assert led_2["converted_opportunity_amount"] == 0.0
    assert led_2["is_won_deal"] is False