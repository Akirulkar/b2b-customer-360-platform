"""Transforms Salesforce Lead Bronze data to silver.lead."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark.silver.utils import compute_record_hash, deduplicate_latest, normalize_email


def transform_lead(bronze_lead_df: DataFrame) -> DataFrame:
    df = bronze_lead_df.filter(F.col("Id").isNotNull())
    df = deduplicate_latest(df, ["Id"], "SystemModstamp")

    silver_df = (
        df.withColumn("lead_id", F.concat(F.lit("led_"), F.col("Id")))
        .withColumn("first_name", F.trim(F.col("FirstName")))
        .withColumn("last_name", F.trim(F.col("LastName")))
        .withColumn("company_name", F.trim(F.col("Company")))
        .withColumn("email", normalize_email("Email"))
        .withColumn("lead_source", F.trim(F.col("LeadSource")))
        .withColumn("status", F.trim(F.col("Status")))
        .withColumn("rating", F.trim(F.col("Rating")))
        .withColumn(
            "owner_id",
            F.when(F.col("OwnerId").isNotNull(), F.concat(F.lit("srep_"), F.col("OwnerId"))).otherwise(F.lit(None)),
        )
        .withColumn("is_converted", F.coalesce(F.col("IsConverted").cast(T.BooleanType()), F.lit(False)))
        .withColumn(
            "converted_account_id",
            F.when(F.col("ConvertedAccountId").isNotNull(), F.concat(F.lit("acc_"), F.col("ConvertedAccountId"))).otherwise(F.lit(None)),
        )
        .withColumn(
            "converted_contact_id",
            F.when(F.col("ConvertedContactId").isNotNull(), F.concat(F.lit("cnt_"), F.col("ConvertedContactId"))).otherwise(F.lit(None)),
        )
        .withColumn(
            "converted_opportunity_id",
            F.when(F.col("ConvertedOpportunityId").isNotNull(), F.concat(F.lit("opp_"), F.col("ConvertedOpportunityId"))).otherwise(F.lit(None)),
        )
        .withColumn("source_system", F.lit("salesforce"))
        .withColumn("source_record_id", F.col("Id"))
        .withColumn("created_at", F.to_utc_timestamp(F.col("CreatedDate"), "UTC"))
        .withColumn("updated_at", F.to_utc_timestamp(F.col("SystemModstamp"), "UTC"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    hash_cols = ["first_name", "last_name", "company_name", "email", "lead_source", "status", "rating", "is_converted"]
    silver_df = silver_df.withColumn("record_hash", compute_record_hash(hash_cols))

    return silver_df.select(
        "lead_id",
        "first_name",
        "last_name",
        "company_name",
        "email",
        "lead_source",
        "status",
        "rating",
        "owner_id",
        "is_converted",
        "converted_account_id",
        "converted_contact_id",
        "converted_opportunity_id",
        "source_system",
        "source_record_id",
        "created_at",
        "updated_at",
        "ingested_at",
        "record_hash",
    )