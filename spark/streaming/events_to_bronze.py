import os
import sys
from pathlib import Path

# Ensure project root is in sys.path so ingestion modules import cleanly
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Set exact JDK 17 & Hadoop environment fallbacks for Windows
if "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot"

if "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = r"C:\hadoop"

os.environ["PATH"] = (
    os.path.join(os.environ["JAVA_HOME"], "bin")
    + os.pathsep
    + os.path.join(os.environ["HADOOP_HOME"], "bin")
    + os.pathsep
    + os.environ.get("PATH", "")
)

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    MapType,
)
from ingestion.utils.logger import logger

def start_streaming_pipeline():
    logger.info(f"PySpark version: {pyspark.__version__}")
    logger.info("Initializing Spark Structured Streaming Session...")

    # PySpark 3.5.x requires the Scala 2.12 Kafka package
    KAFKA_JAR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

    spark = (
        SparkSession.builder.appName("B2B-Customer-Events-Bronze-Ingestion")
        .config("spark.jars.packages", KAFKA_JAR_PACKAGE)
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Contract Schema Definition
    identity_schema = StructType([
        StructField("sfdc_contact_id", StringType(), True),
        StructField("sfdc_account_id", StringType(), True),
        StructField("anonymous_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("domain", StringType(), True),
    ])

    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("identity_resolution", identity_schema, False),
        StructField("attributes", MapType(StringType(), StringType()), True),
    ])

    logger.info("Subscribing to Kafka topic: customer-events...")

    # Read Kafka Stream
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "customer-events")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse and add Bronze Operational Metadata
    parsed_df = (
        kafka_stream.selectExpr(
            "CAST(value AS STRING) as json_payload",
            "offset as _kafka_offset",
            "partition as _kafka_partition",
        )
        .select(
            from_json(col("json_payload"), event_schema).alias("data"),
            "_kafka_offset",
            "_kafka_partition",
        )
        .select("data.*", "_kafka_offset", "_kafka_partition")
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("digital_events"))
    )

    BRONZE_OUTPUT_PATH = "data/bronze/events"
    CHECKPOINT_PATH = "data/checkpoints/events_to_bronze"

    logger.info(f"Starting Bronze stream sink -> {BRONZE_OUTPUT_PATH}")

    query = (
        parsed_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", BRONZE_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="5 seconds")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.warning("Streaming query interrupted by user. Stopping gracefully...")
        query.stop()
    except Exception as e:
        logger.error(f"Streaming query terminated with error: {e}")
        query.stop()
        raise

if __name__ == "__main__":
    start_streaming_pipeline()