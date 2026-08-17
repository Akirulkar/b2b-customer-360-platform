"""tests/gold/test_intent_engagement.py
Test suite for gold.intent_engagement mart.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)
from spark.gold.intent_engagement import build_intent_engagement

def test_intent_engagement_grain_and_scoring(spark):
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("account_id", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("duration_seconds", IntegerType(), True)
    ])

    data = [
        # acc_1 on 2026-08-15: 1 website visit (weight 1), 1 demo request (weight 15) -> intent 16
        ("evt_1", "acc_1", "website_visit", datetime(2026, 8, 15, 10, 0, 0), 30),
        ("evt_2", "acc_1", "demo_request", datetime(2026, 8, 15, 11, 30, 0), 120),
        # acc_1 on 2026-08-16: 2 pricing views (weight 8 each) -> intent 16
        ("evt_3", "acc_1", "pricing_page_view", datetime(2026, 8, 16, 9, 0, 0), 45),
        ("evt_4", "acc_1", "pricing_page_view", datetime(2026, 8, 16, 9, 15, 0), 60),
        # Unresolved event (should be excluded from account mart)
        ("evt_5", None, "website_visit", datetime(2026, 8, 16, 12, 0, 0), 10)
    ]

    events_df = spark.createDataFrame(data, schema)
    result_df = build_intent_engagement(events_df)

    rows = result_df.orderBy("account_id", "event_date").collect()

    # Must produce 2 rows (acc_1 on 2026-08-15 and acc_1 on 2026-08-16)
    assert result_df.count() == 2

    # 2026-08-15 validation
    row_15 = rows[0]
    assert row_15["account_id"] == "acc_1"
    assert str(row_15["event_date"]) == "2026-08-15"
    assert row_15["website_visits"] == 1
    assert row_15["demo_requests"] == 1
    assert row_15["total_duration_seconds"] == 150
    assert row_15["engagement_score"] == 2
    assert row_15["intent_score"] == 16  # 1*1 + 1*15

    # 2026-08-16 validation
    row_16 = rows[1]
    assert row_16["account_id"] == "acc_1"
    assert str(row_16["event_date"]) == "2026-08-16"
    assert row_16["pricing_views"] == 2
    assert row_16["total_duration_seconds"] == 105
    assert row_16["engagement_score"] == 2
    assert row_16["intent_score"] == 16  # 2*8