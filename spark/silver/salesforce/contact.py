"""Transforms Salesforce Contact Bronze data to silver.contact."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark.silver.utils import clean_phone, compute_record_hash, deduplicate_latest, normalize_email


def transform_contact(bronze_contact_df: DataFrame) -> DataFrame:
    df = bronze_contact_df.filter(F.col("Id").isNotNull())
    df = deduplicate_latest(df, ["Id"], "SystemModstamp")

    silver_df = (
        df.withColumn("contact_id", F.concat(F.lit("cnt_"), F.col("Id")))
        .withColumn(
            "account_id",
            F.when(F.col("AccountId").isNotNull(), F.concat(F.lit("acc_"), F.col("AccountId"))).otherwise(F.lit(None)),
        )
        .withColumn("first_name", F.trim(F.col("FirstName")))
        .withColumn("last_name", F.trim(F.col("LastName")))
        .withColumn("email", normalize_email("Email"))
        .withColumn("phone", clean_phone("Phone"))
        .withColumn("job_title", F.trim(F.col("Title")))
        .withColumn("department", F.trim(F.col("Department")))
        .withColumn(
            "owner_id",
            F.when(F.col("OwnerId").isNotNull(), F.concat(F.lit("srep_"), F.col("OwnerId"))).otherwise(F.lit(None)),
        )
        .withColumn("source_system", F.lit("salesforce"))
        .withColumn("source_record_id", F.col("Id"))
        .withColumn("created_at", F.to_utc_timestamp(F.col("CreatedDate"), "UTC"))
        .withColumn("updated_at", F.to_utc_timestamp(F.col("SystemModstamp"), "UTC"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    hash_cols = ["account_id", "first_name", "last_name", "email", "phone", "job_title", "department", "owner_id"]
    silver_df = silver_df.withColumn("record_hash", compute_record_hash(hash_cols))

    return silver_df.select(
        "contact_id",
        "account_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "job_title",
        "department",
        "owner_id",
        "source_system",
        "source_record_id",
        "created_at",
        "updated_at",
        "ingested_at",
        "record_hash",
    )