"""spark/gold/sales_prioritization.py
Generates normalized priority scores and action tiers (Tier 1, Tier 2, Tier 3).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from spark.gold.scoring import calculate_recency_factor, compute_priority_score

def build_sales_prioritization(
    customer_360_df: DataFrame,
    intent_engagement_df: DataFrame,
    as_of_date: str = "2026-08-18"
) -> DataFrame:
    """Calculates prioritized accounts for the sales team."""
    as_of_dt = F.to_date(F.lit(as_of_date))

    # 1. Intent Aggregation over last 14 days
    recent_intent = intent_engagement_df.filter(
        F.col("event_date") >= F.date_sub(as_of_dt, 14)
    ).groupBy("account_id").agg(
        F.sum("intent_score").alias("intent_score_14d"),
        F.sum("engagement_score").alias("engagement_score_14d")
    )

    # 2. Join with Customer 360
    base = customer_360_df.join(recent_intent, on="account_id", how="left").fillna(
        {"intent_score_14d": 0, "engagement_score_14d": 0}
    )

    # 3. Calculate Days Since Last Activity
    with_recency = base.withColumn(
        "days_since_last_activity",
        F.when(
            F.col("last_activity_at").isNotNull(),
            F.datediff(as_of_dt, F.to_date(F.col("last_activity_at")))
        ).otherwise(999)
    ).withColumn(
        "recency_multiplier",
        calculate_recency_factor("days_since_last_activity")
    )

    # 4. Normalization for composite scoring (Min-Max bounded / Log safe)
    # Capped scale metrics:
    # - Max intent expected ~ 100
    # - Max pipeline considered for priority ~ ₹5,000,000
    scored = with_recency.withColumn(
        "norm_intent",
        F.least(F.lit(100.0), (F.col("intent_score_14d") * 100.0) / 50.0)
    ).withColumn(
        "norm_pipeline",
        F.least(F.lit(100.0), (F.col("open_pipeline_value") * 100.0) / 2000000.0)
    ).withColumn(
        "norm_recency",
        F.col("recency_multiplier") * 100.0
    ).withColumn(
        "has_open_opp",
        F.col("open_opportunity_count") > 0
    )

    # 5. Composite Priority Score
    with_final_score = scored.withColumn(
        "priority_score",
        F.round(
            compute_priority_score(
                "norm_intent", "norm_pipeline", "norm_recency", "has_open_opp"
            ), 2
        )
    )

    # 6. Priority Band Classification
    result = with_final_score.withColumn(
        "priority_band",
        F.when(F.col("priority_score") >= 70, "Tier 1")
        .when(F.col("priority_score") >= 40, "Tier 2")
        .otherwise("Tier 3")
    )

    return result.select(
        "account_id",
        "account_name",
        "sales_rep_id",
        "sales_rep_name",
        F.col("intent_score_14d").alias("intent_score"),
        F.col("engagement_score_14d").alias("engagement_score"),
        "open_pipeline_value",
        "open_opportunity_count",
        "days_since_last_activity",
        "priority_score",
        "priority_band"
    )