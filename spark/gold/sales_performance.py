"""spark/gold/sales_performance.py
Computes monthly pipeline conversion and revenue metrics per sales rep.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_sales_performance(
    silver_opps: DataFrame,
    silver_sales_reps: DataFrame
) -> DataFrame:
    """Builds gold.sales_performance analytical table."""
    # Extract year-month format YYYY-MM based on close_date
    opps_monthly = silver_opps.withColumn(
        "month", F.date_format(F.col("close_date"), "yyyy-MM")
    )

    # Rep aggregations per month
    metrics = opps_monthly.groupBy("owner_id", "month").agg(
        F.count("opportunity_id").alias("opportunities_closed"),
        F.sum(F.when(F.col("is_won"), 1).otherwise(0)).alias("won_opportunities"),
        F.sum(F.when(F.col("is_closed") & ~F.col("is_won"), 1).otherwise(0)).alias("lost_opportunities"),
        F.coalesce(F.sum("amount"), F.lit(0.0)).alias("pipeline_closed"),
        F.coalesce(F.sum(F.when(F.col("is_won"), F.col("amount")).otherwise(0)), F.lit(0.0)).alias("won_revenue")
    )

    # Calculate Win Rates and Average Deal Sizes
    reps_clean = silver_sales_reps.select(
        F.col("sales_rep_id").alias("srep_id"),
        F.col("name").alias("sales_rep_name")
    )

    joined = metrics.join(reps_clean, metrics["owner_id"] == reps_clean["srep_id"], how="inner")

    result = joined.withColumn(
        "win_rate",
        F.when(
            F.col("opportunities_closed") > 0,
            F.round((F.col("won_opportunities") * 100.0) / F.col("opportunities_closed"), 2)
        ).otherwise(0.0)
    ).withColumn(
        "average_deal_size",
        F.when(
            F.col("won_opportunities") > 0,
            F.round(F.col("won_revenue") / F.col("won_opportunities"), 2)
        ).otherwise(0.0)
    ).select(
        F.col("owner_id").alias("sales_rep_id"),
        "sales_rep_name",
        "month",
        "opportunities_closed",
        "won_opportunities",
        "lost_opportunities",
        "pipeline_closed",
        "won_revenue",
        "win_rate",
        "average_deal_size"
    )

    return result