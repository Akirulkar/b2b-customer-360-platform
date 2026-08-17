"""scripts/audit_gold_marts.py
Inspect and summarize generated Gold analytical data marts.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.gold.utils import get_spark_session, read_silver_table
from pyspark.sql import functions as F

def audit_gold():
    spark = get_spark_session("AuditGoldMarts")
    
    marts = [
        ("customer_360", "account_id"),
        ("intent_engagement", "account_id"),
        ("sales_prioritization", "account_id"),
        ("sales_performance", "sales_rep_id"),
        ("lead_attribution", "lead_id")
    ]
    
    print("\n" + "="*80)
    print("                      GOLD LAYER AUDIT SUMMARY")
    print("="*80)
    
    for mart_name, key_col in marts:
        df = spark.read.parquet(f"data/gold/{mart_name}")
        count = df.count()
        distinct_keys = df.select(key_col).distinct().count()
        
        print(f"\n[MART: {mart_name.upper()}]")
        print(f" - Total Rows: {count}")
        print(f" - Distinct '{key_col}': {distinct_keys}")
        print(" - Sample Records:")
        df.show(3, truncate=False)
        print("-" * 80)

if __name__ == "__main__":
    audit_gold()