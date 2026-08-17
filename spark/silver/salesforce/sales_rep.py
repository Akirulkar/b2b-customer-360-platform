"""Transforms Salesforce User (+ Role) Bronze data to silver.sales_rep."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark.silver.utils import compute_record_hash, deduplicate_latest, normalize_email


def transform_sales_rep(bronze_user_df: DataFrame) -> DataFrame:
    """
    Schema mapping:
        Id -> sales_rep_id (srep_ + Id)
        Username -> username
        Name -> name
        Email -> email (normalized)
        UserRole.Name / RoleName -> role_name
        IsActive -> is_active
    """
    df = bronze_user_df.filter(F.col("Id").isNotNull())
    df = deduplicate_latest(df, ["Id"], "SystemModstamp")

    role_col = F.col("RoleName") if "RoleName" in df.columns else F.col("UserRole.Name") if "UserRole" in df.columns else F.lit(None).cast(T.StringType())

    silver_df = (
        df.withColumn("sales_rep_id", F.concat(F.lit("srep_"), F.col("Id")))
        .withColumn("username", F.trim(F.col("Username")))
        .withColumn("name", F.trim(F.col("Name")))
        .withColumn("email", normalize_email("Email"))
        .withColumn("role_name", role_col)
        .withColumn("is_active", F.coalesce(F.col("IsActive").cast(T.BooleanType()), F.lit(True)))
        .withColumn("source_system", F.lit("salesforce"))
        .withColumn("source_record_id", F.col("Id"))
        .withColumn("created_at", F.to_utc_timestamp(F.col("CreatedDate"), "UTC"))
        .withColumn("updated_at", F.to_utc_timestamp(F.col("SystemModstamp"), "UTC"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    hash_cols = ["username", "name", "email", "role_name", "is_active"]
    silver_df = silver_df.withColumn("record_hash", compute_record_hash(hash_cols))

    return silver_df.select(
        "sales_rep_id",
        "username",
        "name",
        "email",
        "role_name",
        "is_active",
        "source_system",
        "source_record_id",
        "created_at",
        "updated_at",
        "ingested_at",
        "record_hash",
    )