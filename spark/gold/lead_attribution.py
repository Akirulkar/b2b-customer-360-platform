"""spark/gold/lead_attribution.py
Tracks lead lifecycle conversion rates, velocity, and associated opportunity revenue.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_lead_attribution(
    silver_leads: DataFrame,
    silver_opps: DataFrame
) -> DataFrame:
    """Builds gold.lead_attribution table mapping marketing sources to won revenue."""
    opps_subset = silver_opps.select(
        F.col("opportunity_id"),
        F.col("amount").alias("converted_opportunity_amount"),
        F.col("is_won").alias("opportunity_is_won"),
        F.col("close_date").alias("opportunity_close_date")
    )

    joined = silver_leads.join(
        opps_subset,
        silver_leads["converted_opportunity_id"] == opps_subset["opportunity_id"],
        how="left"
    )

    result = joined.withColumn(
        "conversion_date",
        F.when(F.col("is_converted"), F.to_date(F.col("updated_at"))).otherwise(None)
    ).withColumn(
        "days_to_conversion",
        F.when(
            F.col("is_converted"),
            F.datediff(F.to_date(F.col("updated_at")), F.to_date(F.col("created_at")))
        ).otherwise(None)
    ).select(
        "lead_id",
        "lead_source",
        "status",
        "rating",
        "created_at",
        "is_converted",
        "converted_account_id",
        "converted_contact_id",
        "converted_opportunity_id",
        "conversion_date",
        "days_to_conversion",
        F.coalesce("converted_opportunity_amount", F.lit(0.0)).alias("converted_opportunity_amount"),
        F.coalesce("opportunity_is_won", F.lit(False)).alias("is_won_deal")
    )

    return result