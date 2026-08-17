"""Transforms OpportunityContactRole Bronze data to silver.opportunity_contact_role."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark.silver.utils import compute_record_hash, deduplicate_latest


def transform_opportunity_contact_role(bronze_ocr_df: DataFrame) -> DataFrame:
    df = bronze_ocr_df.filter(F.col("Id").isNotNull())
    df = deduplicate_latest(df, ["Id"], "SystemModstamp")

    silver_df = (
        df.withColumn("opportunity_contact_role_id", F.concat(F.lit("ocr_"), F.col("Id")))
        .withColumn(
            "opportunity_id",
            F.when(F.col("OpportunityId").isNotNull(), F.concat(F.lit("opp_"), F.col("OpportunityId"))).otherwise(F.lit(None)),
        )
        .withColumn(
            "contact_id",
            F.when(F.col("ContactId").isNotNull(), F.concat(F.lit("cnt_"), F.col("ContactId"))).otherwise(F.lit(None)),
        )
        .withColumn("role", F.trim(F.col("Role")))
        .withColumn("is_primary", F.coalesce(F.col("IsPrimary").cast(T.BooleanType()), F.lit(False)))
        .withColumn("source_system", F.lit("salesforce"))
        .withColumn("source_record_id", F.col("Id"))
        .withColumn("created_at", F.to_utc_timestamp(F.col("CreatedDate"), "UTC"))
        .withColumn("updated_at", F.to_utc_timestamp(F.col("SystemModstamp"), "UTC"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    hash_cols = ["opportunity_id", "contact_id", "role", "is_primary"]
    silver_df = silver_df.withColumn("record_hash", compute_record_hash(hash_cols))

    return silver_df.select(
        "opportunity_contact_role_id",
        "opportunity_id",
        "contact_id",
        "role",
        "is_primary",
        "source_system",
        "source_record_id",
        "created_at",
        "updated_at",
        "ingested_at",
        "record_hash",
    )