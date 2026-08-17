"""Transforms Salesforce Account Bronze data to silver.account."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark.silver.utils import clean_phone, compute_record_hash, deduplicate_latest, normalize_domain


def transform_account(bronze_account_df: DataFrame) -> DataFrame:
    df = bronze_account_df.filter(F.col("Id").isNotNull())
    df = deduplicate_latest(df, ["Id"], "SystemModstamp")

    silver_df = (
        df.withColumn("account_id", F.concat(F.lit("acc_"), F.col("Id")))
        .withColumn("account_name", F.trim(F.col("Name")))
        .withColumn("account_type", F.trim(F.col("Type")))
        .withColumn("phone", clean_phone("Phone"))
        .withColumn("website", normalize_domain("Website"))
        .withColumn("industry", F.trim(F.col("Industry")))
        .withColumn("annual_revenue", F.col("AnnualRevenue").cast(T.DecimalType(18, 2)))
        .withColumn("employee_count", F.col("NumberOfEmployees").cast(T.IntegerType()))
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

    hash_cols = ["account_name", "account_type", "phone", "website", "industry", "annual_revenue", "employee_count", "owner_id"]
    silver_df = silver_df.withColumn("record_hash", compute_record_hash(hash_cols))

    return silver_df.select(
        "account_id",
        "account_name",
        "account_type",
        "phone",
        "website",
        "industry",
        "annual_revenue",
        "employee_count",
        "owner_id",
        "source_system",
        "source_record_id",
        "created_at",
        "updated_at",
        "ingested_at",
        "record_hash",
    )