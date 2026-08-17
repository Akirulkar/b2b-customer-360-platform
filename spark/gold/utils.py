"""spark/gold/utils.py
Common utility functions for Gold Layer transformations on Windows.
"""
import os
import sys
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Configure Windows Paths
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot"
HADOOP_PATH = r"C:\hadoop"

if "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = JAVA_PATH

if "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = HADOOP_PATH

os.environ["PATH"] = (
    os.path.join(JAVA_PATH, "bin") + os.pathsep +
    os.path.join(HADOOP_PATH, "bin") + os.pathsep +
    os.environ.get("PATH", "")
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


def get_spark_session(app_name: str = "B2B_Gold_Pipeline") -> SparkSession:
    """Builds or retrieves a SparkSession with Windows local configs."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate()
    )

def read_silver_table(
    spark: SparkSession,
    table_name: str,
    base_path: str = "data/silver"
) -> DataFrame:
    """Reads a Silver layer table stored in Parquet format."""
    path = os.path.join(base_path, table_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Silver table not found at path: {path}")
    return spark.read.parquet(path)

def write_gold_table(
    df: DataFrame,
    mart_name: str,
    base_path: str = "data/gold",
    partition_cols: Optional[list] = None,
    mode: str = "overwrite"
) -> None:
    """Writes a Gold data mart as Parquet with timestamp metadata."""
    out_path = os.path.join(base_path, mart_name)
    enriched_df = df.withColumn("gold_processed_at", F.current_timestamp())
    
    writer = enriched_df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(out_path)
    print(f"[GOLD] Wrote {enriched_df.count()} records to {out_path}")