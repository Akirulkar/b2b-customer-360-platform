"""Deterministic Identity Resolution Engine implementing the Phase 2 5-tier waterfall."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark.silver.config import PUBLIC_EMAIL_DOMAINS
from spark.silver.utils import normalize_domain, normalize_email


def build_or_update_identity_map(
    events_df: DataFrame,
    contacts_df: DataFrame,
    accounts_df: DataFrame,
    existing_identity_df: DataFrame = None,
) -> DataFrame:
    """
    Extracts identity tuples from streaming events and CRM tables, resolves them
    via the 5-tier deterministic waterfall, and generates silver.identity.
    """
    public_domains_lit = F.array([F.lit(d) for d in PUBLIC_EMAIL_DOMAINS])

    # Extract raw identity linkages from digital events
    event_identities = events_df.select(
        F.col("identity_resolution.anonymous_id").alias("anonymous_id"),
        normalize_email("identity_resolution.email").alias("email"),
        normalize_domain("identity_resolution.domain").alias("domain"),
        F.col("identity_resolution.sfdc_contact_id").alias("sfdc_contact_id"),
        F.col("identity_resolution.sfdc_account_id").alias("sfdc_account_id"),
        F.to_utc_timestamp(F.col("timestamp"), "UTC").alias("event_ts"),
    )

    # CRM Contact identities
    contact_identities = contacts_df.select(
        F.lit(None).cast(T.StringType()).alias("anonymous_id"),
        F.col("email").alias("email"),
        normalize_domain("email").alias("domain"),
        F.col("source_record_id").alias("sfdc_contact_id"),
        F.regexp_replace(F.col("account_id"), r"^acc_", "").alias("sfdc_account_id"),
        F.col("updated_at").alias("event_ts"),
    ).filter(F.col("email").isNotNull())

    # CRM Account domain identities
    account_identities = accounts_df.select(
        F.lit(None).cast(T.StringType()).alias("anonymous_id"),
        F.lit(None).cast(T.StringType()).alias("email"),
        normalize_domain("website").alias("domain"),
        F.lit(None).cast(T.StringType()).alias("sfdc_contact_id"),
        F.col("source_record_id").alias("sfdc_account_id"),
        F.col("updated_at").alias("event_ts"),
    ).filter(F.col("website").isNotNull())

    combined_raw = event_identities.unionByName(contact_identities).unionByName(account_identities)

    if existing_identity_df is not None and not existing_identity_df.isEmpty():
        existing_raw = existing_identity_df.select(
            "anonymous_id",
            "email",
            "domain",
            F.regexp_replace("contact_id", r"^cnt_", "").alias("sfdc_contact_id"),
            F.regexp_replace("account_id", r"^acc_", "").alias("sfdc_account_id"),
            F.col("last_seen_at").alias("event_ts"),
        )
        combined_raw = combined_raw.unionByName(existing_raw)

    # Clean domain if it is a public email provider
    combined_raw = combined_raw.withColumn(
        "domain",
        F.when(F.array_contains(public_domains_lit, F.col("domain")), F.lit(None)).otherwise(F.col("domain")),
    )

    # Join lookups
    contacts_lookup = contacts_df.select(
        F.col("contact_id").alias("matched_contact_id_by_email"),
        F.col("account_id").alias("matched_account_id_by_email"),
        F.col("email").alias("c_email"),
    )
    accounts_lookup = accounts_df.select(
        F.col("account_id").alias("matched_account_id_by_domain"),
        normalize_domain("website").alias("a_domain"),
    ).filter(~F.array_contains(public_domains_lit, F.col("a_domain")))

    enriched = combined_raw.join(
        contacts_lookup, combined_raw.email == contacts_lookup.c_email, "left"
    ).join(
        accounts_lookup, combined_raw.domain == accounts_lookup.a_domain, "left"
    )

    # 5-Tier Deterministic Waterfall Resolution
    resolved = (
        enriched.withColumn(
            "resolved_contact_id",
            F.when(
                F.col("sfdc_contact_id").isNotNull(),
                F.concat(F.lit("cnt_"), F.col("sfdc_contact_id")),
            ).when(
                F.col("matched_contact_id_by_email").isNotNull(),
                F.col("matched_contact_id_by_email"),
            ).otherwise(F.lit(None).cast(T.StringType())),
        )
        .withColumn(
            "resolved_account_id",
            F.when(
                F.col("sfdc_account_id").isNotNull(),
                F.concat(F.lit("acc_"), F.col("sfdc_account_id")),
            ).when(
                F.col("matched_account_id_by_email").isNotNull(),
                F.col("matched_account_id_by_email"),
            ).when(
                F.col("matched_account_id_by_domain").isNotNull(),
                F.col("matched_account_id_by_domain"),
            ).otherwise(F.lit(None).cast(T.StringType())),
        )
    )

    # Group by identity key tuple (anonymous_id, email, domain) to aggregate timestamps and stitch
    grouped = resolved.groupBy("anonymous_id", "email", "domain").agg(
        F.first("resolved_contact_id", ignorenulls=True).alias("contact_id"),
        F.first("resolved_account_id", ignorenulls=True).alias("account_id"),
        F.min("event_ts").alias("first_seen_at"),
        F.max("event_ts").alias("last_seen_at"),
    )

    # Stitch anonymous identities retroactively where anonymous_id matches a known email/account
    anon_window = Window.partitionBy("anonymous_id").orderBy(F.col("contact_id").desc_nulls_last(), F.col("account_id").desc_nulls_last())
    stitched = grouped.withColumn(
        "stitched_contact_id", F.first("contact_id", ignorenulls=True).over(anon_window)
    ).withColumn(
        "stitched_account_id", F.first("account_id", ignorenulls=True).over(anon_window)
    ).drop("contact_id", "account_id").withColumnRenamed(
        "stitched_contact_id", "contact_id"
    ).withColumnRenamed(
        "stitched_account_id", "account_id"
    )

    identity_df = (
        stitched.filter(F.col("anonymous_id").isNotNull() | F.col("email").isNotNull() | F.col("domain").isNotNull())
        .withColumn(
            "identity_id",
            F.concat(
                F.lit("idn_"),
                F.sha2(F.concat_ws("||", F.coalesce(F.col("anonymous_id"), F.lit("")), F.coalesce(F.col("email"), F.lit(""))), 256),
            ),
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    return identity_df.select(
        "identity_id",
        "anonymous_id",
        "email",
        "domain",
        "contact_id",
        "account_id",
        "first_seen_at",
        "last_seen_at",
        "updated_at",
    )