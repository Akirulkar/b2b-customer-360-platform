"""spark/gold/customer_360.py
Aggregates Account, Contact, Opportunity, Sales Rep, and Digital Event data.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def build_customer_360(
    silver_accounts: DataFrame,
    silver_contacts: DataFrame,
    silver_opps: DataFrame,
    silver_sales_reps: DataFrame,
    silver_events: DataFrame,
    as_of_date: str = "2026-08-18"
) -> DataFrame:
    """Constructs gold.customer_360 table."""
    # 1. Contact Metrics per Account
    contacts_agg = silver_contacts.groupBy("account_id").agg(
        F.count("contact_id").alias("contact_count")
    )

    # 2. Opportunity Metrics per Account
    opps_agg = silver_opps.groupBy("account_id").agg(
        F.sum(F.when(~F.col("is_closed"), 1).otherwise(0)).alias("open_opportunity_count"),
        F.coalesce(F.sum(F.when(~F.col("is_closed"), F.col("amount")).otherwise(0)), F.lit(0.0)).alias("open_pipeline_value"),
        F.sum(F.when(F.col("is_won"), 1).otherwise(0)).alias("won_opportunity_count"),
        F.coalesce(F.sum(F.when(F.col("is_won"), F.col("amount")).otherwise(0)), F.lit(0.0)).alias("won_pipeline_value"),
        F.sum(F.when(F.col("is_closed") & ~F.col("is_won"), 1).otherwise(0)).alias("lost_opportunity_count")
    )

    # 3. Digital Event Activity (7d, 30d, and Lifetime Recency)
    as_of_ts = F.to_timestamp(F.lit(as_of_date))
    events_agg = silver_events.filter(F.col("account_id").isNotNull()).groupBy("account_id").agg(
        F.max("event_timestamp").alias("last_activity_at"),
        F.sum(
            F.when(
                F.col("event_timestamp") >= F.date_sub(as_of_ts, 7), 1
            ).otherwise(0)
        ).alias("events_last_7d"),
        F.sum(
            F.when(
                F.col("event_timestamp") >= F.date_sub(as_of_ts, 30), 1
            ).otherwise(0)
        ).alias("events_last_30d")
    )

    # 4. Sales Rep Details
    reps = silver_sales_reps.select(
        F.col("sales_rep_id").alias("rep_id"),
        F.col("name").alias("sales_rep_name"),
        F.col("email").alias("sales_rep_email")
    )

    # 5. Master Join on Accounts
    c360 = (
        silver_accounts
        .join(contacts_agg, on="account_id", how="left")
        .join(opps_agg, on="account_id", how="left")
        .join(events_agg, on="account_id", how="left")
        .join(reps, silver_accounts["owner_id"] == reps["rep_id"], how="left")
        .select(
            silver_accounts["account_id"],
            silver_accounts["account_name"],
            silver_accounts["industry"],
            silver_accounts["employee_count"],
            silver_accounts["annual_revenue"],
            silver_accounts["website"],
            F.coalesce(contacts_agg["contact_count"], F.lit(0)).alias("contact_count"),
            F.coalesce(opps_agg["open_opportunity_count"], F.lit(0)).alias("open_opportunity_count"),
            F.coalesce(opps_agg["open_pipeline_value"], F.lit(0.0)).alias("open_pipeline_value"),
            F.coalesce(opps_agg["won_opportunity_count"], F.lit(0)).alias("won_opportunity_count"),
            F.coalesce(opps_agg["won_pipeline_value"], F.lit(0.0)).alias("won_pipeline_value"),
            F.coalesce(opps_agg["lost_opportunity_count"], F.lit(0)).alias("lost_opportunity_count"),
            events_agg["last_activity_at"],
            F.coalesce(events_agg["events_last_7d"], F.lit(0)).alias("events_last_7d"),
            F.coalesce(events_agg["events_last_30d"], F.lit(0)).alias("events_last_30d"),
            silver_accounts["owner_id"].alias("sales_rep_id"),
            reps["sales_rep_name"],
            reps["sales_rep_email"]
        )
    )
    return c360