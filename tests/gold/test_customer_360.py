"""tests/gold/test_customer_360.py
Validation suite for customer_360 mart.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

"""tests/gold/test_customer_360.py"""
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
)
from spark.gold.customer_360 import build_customer_360

def test_customer_360_grain_and_metrics(spark):
    acc_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("employee_count", IntegerType(), True),
        StructField("annual_revenue", DoubleType(), True),
        StructField("website", StringType(), True),
        StructField("owner_id", StringType(), True)
    ])
    acc_data = [("acc_1", "Acme Corp", "Tech", 100, 1000000.0, "https://acme.com", "srep_1")]
    accounts = spark.createDataFrame(acc_data, acc_schema)

    cnt_schema = StructType([
        StructField("contact_id", StringType(), False),
        StructField("account_id", StringType(), True)
    ])
    contacts = spark.createDataFrame([("cnt_1", "acc_1"), ("cnt_2", "acc_1")], cnt_schema)

    opp_schema = StructType([
        StructField("opportunity_id", StringType(), False),
        StructField("account_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("is_closed", BooleanType(), True),
        StructField("is_won", BooleanType(), True)
    ])
    opps = spark.createDataFrame([
        ("opp_1", "acc_1", 50000.0, False, False),
        ("opp_2", "acc_1", 25000.0, True, True)
    ], opp_schema)

    srep_schema = StructType([
        StructField("sales_rep_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ])
    reps = spark.createDataFrame([("srep_1", "John Doe", "john@company.com")], srep_schema)

    evt_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("account_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True)
    ])
    events = spark.createDataFrame([("evt_1", "acc_1", datetime(2026, 8, 15, 10, 0, 0))], evt_schema)

    c360 = build_customer_360(accounts, contacts, opps, reps, events, as_of_date="2026-08-18")
    row = c360.collect()[0]

    assert c360.count() == 1
    assert row["contact_count"] == 2
    assert row["open_opportunity_count"] == 1
    assert row["open_pipeline_value"] == 50000.0
    assert row["won_opportunity_count"] == 1
    assert row["won_pipeline_value"] == 25000.0
    assert row["events_last_7d"] == 1
    assert row["sales_rep_name"] == "John Doe"