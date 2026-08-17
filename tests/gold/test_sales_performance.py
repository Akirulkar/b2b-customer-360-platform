"""tests/gold/test_sales_performance.py
Test suite for gold.sales_performance mart.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import date
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, DateType
)
from spark.gold.sales_performance import build_sales_performance

def test_sales_performance_aggregations_and_win_rate(spark):
    opp_schema = StructType([
        StructField("opportunity_id", StringType(), False),
        StructField("owner_id", StringType(), False),
        StructField("amount", DoubleType(), True),
        StructField("is_closed", BooleanType(), True),
        StructField("is_won", BooleanType(), True),
        StructField("close_date", DateType(), True)
    ])

    rep_schema = StructType([
        StructField("sales_rep_id", StringType(), False),
        StructField("name", StringType(), True)
    ])

    opp_data = [
        # Rep 1: 2026-08 - 2 closed deals (1 won at 100k, 1 lost at 50k) -> 50% win rate
        ("opp_1", "srep_1", 100000.0, True, True, date(2026, 8, 10)),
        ("opp_2", "srep_1", 50000.0, True, False, date(2026, 8, 14)),
        # Rep 1: 2026-07 - 1 deal won (80k) -> 100% win rate
        ("opp_3", "srep_1", 80000.0, True, True, date(2026, 7, 20)),
        # Rep 2: 2026-08 - 1 deal lost (30k) -> 0% win rate
        ("opp_4", "srep_2", 30000.0, True, False, date(2026, 8, 5))
    ]

    rep_data = [
        ("srep_1", "Sarah Connor"),
        ("srep_2", "Kyle Reese")
    ]

    opps_df = spark.createDataFrame(opp_data, opp_schema)
    reps_df = spark.createDataFrame(rep_data, rep_schema)

    result_df = build_sales_performance(opps_df, reps_df)
    rows = result_df.orderBy("sales_rep_id", "month").collect()

    # Expecting 3 rows: (srep_1, 2026-07), (srep_1, 2026-08), (srep_2, 2026-08)
    assert result_df.count() == 3

    # srep_1 in 2026-08
    r1_aug = [r for r in rows if r["sales_rep_id"] == "srep_1" and r["month"] == "2026-08"][0]
    assert r1_aug["sales_rep_name"] == "Sarah Connor"
    assert r1_aug["opportunities_closed"] == 2
    assert r1_aug["won_opportunities"] == 1
    assert r1_aug["lost_opportunities"] == 1
    assert r1_aug["pipeline_closed"] == 150000.0
    assert r1_aug["won_revenue"] == 100000.0
    assert r1_aug["win_rate"] == 50.0
    assert r1_aug["average_deal_size"] == 100000.0

    # srep_2 in 2026-08
    r2_aug = [r for r in rows if r["sales_rep_id"] == "srep_2" and r["month"] == "2026-08"][0]
    assert r2_aug["opportunities_closed"] == 1
    assert r2_aug["won_opportunities"] == 0
    assert r2_aug["won_revenue"] == 0.0
    assert r2_aug["win_rate"] == 0.0
    assert r2_aug["average_deal_size"] == 0.0