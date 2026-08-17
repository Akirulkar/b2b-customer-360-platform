"""spark/gold/intent_engagement.py
Calculates daily behavioral rollups and weighted intent scores.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from spark.gold.scoring import get_event_weight_expr

def build_intent_engagement(silver_events: DataFrame) -> DataFrame:
    """Transforms raw digital events into daily account-level intent metrics."""
    # Filter resolved accounts
    resolved_events = silver_events.filter(F.col("account_id").isNotNull())

    # Add weight per event
    weighted_events = resolved_events.withColumn(
        "event_date", F.to_date(F.col("event_timestamp"))
    ).withColumn(
        "weight", get_event_weight_expr("event_type")
    )

    # Aggregate by account and date
    mart = weighted_events.groupBy("account_id", "event_date").agg(
        F.sum(F.when(F.col("event_type") == "website_visit", 1).otherwise(0)).alias("website_visits"),
        F.sum(F.when(F.col("event_type") == "product_page_view", 1).otherwise(0)).alias("product_views"),
        F.sum(F.when(F.col("event_type") == "documentation_view", 1).otherwise(0)).alias("documentation_views"),
        F.sum(F.when(F.col("event_type") == "brochure_download", 1).otherwise(0)).alias("brochure_downloads"),
        F.sum(F.when(F.col("event_type") == "pricing_page_view", 1).otherwise(0)).alias("pricing_views"),
        F.sum(F.when(F.col("event_type") == "demo_request", 1).otherwise(0)).alias("demo_requests"),
        F.coalesce(F.sum("duration_seconds"), F.lit(0)).alias("total_duration_seconds"),
        F.count("event_id").alias("engagement_score"),  # Total interactions
        F.sum("weight").alias("intent_score")           # Weighted business score
    )

    return mart.select(
        "account_id",
        "event_date",
        "website_visits",
        "product_views",
        "pricing_views",
        "documentation_views",
        "brochure_downloads",
        "demo_requests",
        "total_duration_seconds",
        "engagement_score",
        "intent_score"
    )