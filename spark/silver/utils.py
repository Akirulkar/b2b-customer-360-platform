"""Reusable Spark transformation utilities for normalization, hashing, and deduplication."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def normalize_email(col_name: str) -> Column:
    """Trims whitespace and lowercases email strings."""
    return F.lower(F.trim(F.col(col_name)))


def normalize_domain(col_name: str) -> Column:
    """Extracts base domain from a website URL or email address."""
    clean_url = F.lower(F.trim(F.col(col_name)))
    clean_url = F.regexp_replace(clean_url, r"^https?://(www\.)?", "")
    clean_url = F.regexp_replace(clean_url, r"/.*$", "")
    return clean_url


def clean_phone(col_name: str) -> Column:
    """Strips all non-numeric characters except preserving a leading '+' sign."""
    trimmed_col = F.trim(F.col(col_name))
    has_plus = trimmed_col.startswith("+")
    digits_only = F.regexp_replace(F.col(col_name), r"[^\d]", "")
    
    return F.when(
        F.col(col_name).isNull(), F.lit(None).cast(T.StringType())
    ).otherwise(
        F.when(has_plus, F.concat(F.lit("+"), digits_only)).otherwise(digits_only)
    )


def compute_record_hash(cols: list[str]) -> Column:
    """Calculates deterministic SHA-256 hash across business columns."""
    coalesced_cols = [F.coalesce(F.col(c).cast(T.StringType()), F.lit("")) for c in cols]
    return F.sha2(F.concat_ws("||", *coalesced_cols), 256)


def deduplicate_latest(df: DataFrame, primary_keys: list[str], order_by_col: str) -> DataFrame:
    """Deduplicates a DataFrame keeping the most recently updated record."""
    window = Window.partitionBy(*primary_keys).orderBy(F.col(order_by_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )