"""Canonicalizes Bronze digital events and enriches them via silver.identity resolution."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark.silver.config import INTERACTION_WEIGHTS
from spark.silver.utils import compute_record_hash, deduplicate_latest, normalize_domain, normalize_email


def transform_customer_events(
    bronze_events_df: DataFrame,
    identity_map_df: DataFrame,
) -> DataFrame:
    """
    Transforms Bronze Kafka events into canonical silver.customer_event with UTC normalization,
    interaction scoring tier categorization, deduplication, and resolved identity FKs.
    """
    # 1. Deduplicate events at the bronze event_id grain
    events_dedup = deduplicate_latest(bronze_events_df, ["event_id"], "timestamp")

    # 2. Flatten & normalize raw payload
    flattened = (
        events_dedup.withColumn("event_timestamp", F.to_utc_timestamp(F.col("timestamp"), "UTC"))
        .withColumn("anonymous_id", F.col("identity_resolution.anonymous_id"))
        .withColumn("email", normalize_email("identity_resolution.email"))
        .withColumn("domain", normalize_domain("identity_resolution.domain"))
        .withColumn("url", F.col("attributes.url"))
        .withColumn("plan_interest", F.col("attributes.plan_interest"))
        .withColumn("duration_seconds", F.col("attributes.duration_seconds").cast(T.IntegerType()))
        .withColumn("attributes", F.to_json(F.col("attributes")))
        .withColumn("source_system", F.lit("digital_events"))
        .withColumn("source_record_id", F.col("event_id"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    # 3. Add standardized interaction weight category
    weight_mapping_expr = F.create_map([F.lit(x) for kv in INTERACTION_WEIGHTS.items() for x in kv])
    flattened = flattened.withColumn(
        "interaction_weight",
        F.coalesce(weight_mapping_expr[F.col("event_type")], F.lit("low")),
    )

    # 4. Join with Identity Map (Preference: Anonymous ID match -> Email match)
    anon_map = identity_map_df.filter(F.col("anonymous_id").isNotNull()).select(
        F.col("anonymous_id").alias("m_anon_id"),
        F.col("contact_id").alias("anon_contact_id"),
        F.col("account_id").alias("anon_account_id"),
    ).dropDuplicates(["m_anon_id"])

    email_map = identity_map_df.filter(F.col("email").isNotNull()).select(
        F.col("email").alias("m_email"),
        F.col("contact_id").alias("email_contact_id"),
        F.col("account_id").alias("email_account_id"),
    ).dropDuplicates(["m_email"])

    joined = flattened.join(
        anon_map, flattened.anonymous_id == anon_map.m_anon_id, "left"
    ).join(
        email_map, flattened.email == email_map.m_email, "left"
    )

    # Finalize resolved Foreign Keys
    silver_events = (
        joined.withColumn(
            "contact_id",
            F.coalesce(F.col("anon_contact_id"), F.col("email_contact_id")),
        )
        .withColumn(
            "account_id",
            F.coalesce(F.col("anon_account_id"), F.col("email_account_id")),
        )
    )

    hash_cols = [
        "event_type",
        "event_timestamp",
        "anonymous_id",
        "email",
        "contact_id",
        "account_id",
        "url",
        "duration_seconds",
    ]
    silver_events = silver_events.withColumn("record_hash", compute_record_hash(hash_cols))

    return silver_events.select(
        "event_id",
        "event_type",
        "event_timestamp",
        "contact_id",
        "account_id",
        "anonymous_id",
        "email",
        "domain",
        "url",
        "plan_interest",
        "duration_seconds",
        "interaction_weight",
        "attributes",
        "source_system",
        "source_record_id",
        "ingested_at",
        "record_hash",
    )