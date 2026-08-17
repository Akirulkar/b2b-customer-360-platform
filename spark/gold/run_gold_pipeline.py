"""spark/gold/run_gold_pipeline.py
Master execution pipeline for Gold marts.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.gold.utils import get_spark_session, read_silver_table, write_gold_table
from spark.gold.customer_360 import build_customer_360
from spark.gold.intent_engagement import build_intent_engagement
from spark.gold.sales_prioritization import build_sales_prioritization
from spark.gold.sales_performance import build_sales_performance
from spark.gold.lead_attribution import build_lead_attribution

def run_pipeline():
    spark = get_spark_session("Phase6_Gold_Pipeline")
    print("=== Starting Gold Mart Transformations ===")

    # 1. Ingest Silver Entities
    accounts = read_silver_table(spark, "account")
    contacts = read_silver_table(spark, "contact")
    opps = read_silver_table(spark, "opportunity")
    sales_reps = read_silver_table(spark, "sales_rep")
    events = read_silver_table(spark, "customer_event")
    leads = read_silver_table(spark, "lead")

    # 2. Build Customer 360
    print("[1/5] Processing Customer 360...")
    c360_df = build_customer_360(accounts, contacts, opps, sales_reps, events)
    write_gold_table(c360_df, "customer_360")

    # 3. Build Intent & Engagement
    print("[2/5] Processing Intent & Engagement...")
    intent_df = build_intent_engagement(events)
    write_gold_table(intent_df, "intent_engagement", partition_cols=["event_date"])

    # 4. Build Sales Prioritization (Depends on C360 + Intent)
    print("[3/5] Processing Sales Prioritization...")
    prioritization_df = build_sales_prioritization(c360_df, intent_df)
    write_gold_table(prioritization_df, "sales_prioritization")

    # 5. Build Sales Performance
    print("[4/5] Processing Sales Performance...")
    perf_df = build_sales_performance(opps, sales_reps)
    write_gold_table(perf_df, "sales_performance")

    # 6. Build Lead Attribution
    print("[5/5] Processing Lead Attribution...")
    attr_df = build_lead_attribution(leads, opps)
    write_gold_table(attr_df, "lead_attribution")

    print("=== Phase 6 Gold Pipeline Completed Successfully ===")

if __name__ == "__main__":
    run_pipeline()