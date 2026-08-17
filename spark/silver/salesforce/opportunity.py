"""Transforms Salesforce Opportunity Bronze data to silver.opportunity."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark.silver.utils import compute_record_hash, deduplicate_latest


def transform_opportunity(bronze_opportunity_df: DataFrame) -> DataFrame:
    df = bronze_opportunity_df.filter(F.col("Id").isNotNull())
    df = deduplicate_latest(df, ["Id"], "SystemModstamp")

    silver_df = (
        df.withColumn("opportunity_id", F.concat(F.lit("opp_"), F.col("Id")))
        .withColumn(
            "account_id",
            F.when(F.col("AccountId").isNotNull(), F.concat(F.lit("acc_"), F.col("AccountId"))).otherwise(F.lit(None)),
        )
        .withColumn("opportunity_name", F.trim(F.col("Name")))
        .withColumn("stage", F.trim(F.col("StageName")))
        .withColumn("amount", F.col("Amount").cast(T.DecimalType(18, 2)))
        .withColumn("probability", F.col("Probability").cast(T.DecimalType(5, 2)))
        .withColumn("close_date", F.to_date(F.col("CloseDate")))
        .withColumn("opportunity_type", F.trim(F.col("Type")))
        .withColumn("lead_source", F.trim(F.col("LeadSource")))
        .withColumn("is_closed", F.coalesce(F.col("IsClosed").cast(T.BooleanType()), F.lit(False)))
        .withColumn("is_won", F.coalesce(F.col("IsWon").cast(T.BooleanType()), F.lit(False)))
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

    hash_cols = ["account_id", "opportunity_name", "stage", "amount", "probability", "close_date", "opportunity_type", "is_closed", "is_won", "owner_id"]
    silver_df = silver_df.withColumn("record_hash", compute_record_hash(hash_cols))

    return silver_df.select(
        "opportunity_id",
        "account_id",
        "opportunity_name",
        "stage",
        "amount",
        "probability",
        "close_date",
        "opportunity_type",
        "lead_source",
        "is_closed",
        "is_won",
        "owner_id",
        "source_system",
        "source_record_id",
        "created_at",
        "updated_at",
        "ingested_at",
        "record_hash",
    )