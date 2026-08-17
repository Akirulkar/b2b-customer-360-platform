"""End-to-end orchestration runner for Silver canonical layer."""

import os
import sys
from pathlib import Path

# 1. Resolve project root and add to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 2. Configure Windows Java and Hadoop environments
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot"
HADOOP_PATH = r"C:\hadoop"

if os.path.exists(JAVA_PATH):
    os.environ["JAVA_HOME"] = JAVA_PATH
if os.path.exists(HADOOP_PATH):
    os.environ["HADOOP_HOME"] = HADOOP_PATH

# 3. Add Java and Hadoop bin folders to PATH
java_bin = os.path.join(os.environ.get("JAVA_HOME", ""), "bin")
hadoop_bin = os.path.join(os.environ.get("HADOOP_HOME", ""), "bin")
os.environ["PATH"] = f"{java_bin};{hadoop_bin};" + os.environ.get("PATH", "")

# 4. Bind Python workers directly to the active venv executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T
from spark.silver.config import BRONZE_BASE_PATH, SILVER_BASE_PATH
from spark.silver.events.customer_event import transform_customer_events
from spark.silver.identity.resolve_identity import build_or_update_identity_map
from spark.silver.salesforce.account import transform_account
from spark.silver.salesforce.contact import transform_contact
from spark.silver.salesforce.lead import transform_lead
from spark.silver.salesforce.opportunity import transform_opportunity
from spark.silver.salesforce.opportunity_contact_role import transform_opportunity_contact_role
from spark.silver.salesforce.sales_rep import transform_sales_rep


def load_bronze_table_or_empty(spark: SparkSession, possible_paths: list[str], fallback_schema: T.StructType) -> DataFrame:
    """Attempts to read from multiple possible bronze paths, returning an empty DF if none exist."""
    for p in possible_paths:
        full_path = os.path.normpath(os.path.join(PROJECT_ROOT, p))
        if os.path.exists(full_path):
            print(f"  -> Found: {p}")
            return spark.read.parquet(full_path)
    print(f"  -> Path not found in {possible_paths}. Initializing empty schema fallback.")
    return spark.createDataFrame([], schema=fallback_schema)


def validate_referential_integrity(
    contacts_df: DataFrame,
    accounts_df: DataFrame,
    opps_df: DataFrame,
    ocrs_df: DataFrame,
    reps_df: DataFrame,
):
    """Logs and asserts foreign-key consistency across canonical Silver tables."""
    print("\n--- Running Silver Referential Integrity Validations ---")

    # 1. Contact -> Account
    valid_accs = accounts_df.select("account_id").distinct()
    orphan_contacts = contacts_df.filter(contacts_df.account_id.isNotNull()).join(
        valid_accs, "account_id", "left_anti"
    ).count()
    print(f"Orphan Contacts (invalid account_id): {orphan_contacts}")

    # 2. Opportunity -> Account
    orphan_opps = opps_df.filter(opps_df.account_id.isNotNull()).join(
        valid_accs, "account_id", "left_anti"
    ).count()
    print(f"Orphan Opportunities (invalid account_id): {orphan_opps}")

    # 3. OCR -> Opportunity & Contact
    if not ocrs_df.isEmpty():
        valid_opps = opps_df.select("opportunity_id").distinct()
        valid_cnts = contacts_df.select("contact_id").distinct()
        orphan_ocr_opp = ocrs_df.filter(ocrs_df.opportunity_id.isNotNull()).join(
            valid_opps, "opportunity_id", "left_anti"
        ).count()
        orphan_ocr_cnt = ocrs_df.filter(ocrs_df.contact_id.isNotNull()).join(
            valid_cnts, "contact_id", "left_anti"
        ).count()
        print(f"Orphan OCRs (invalid opp_id: {orphan_ocr_opp}, invalid contact_id: {orphan_ocr_cnt})")
    else:
        print("Orphan OCRs: Skipped (table is empty)")


def run_silver_pipeline(spark: SparkSession):
    ocr_schema = T.StructType([
        T.StructField("Id", T.StringType(), True),
        T.StructField("OpportunityId", T.StringType(), True),
        T.StructField("ContactId", T.StringType(), True),
        T.StructField("Role", T.StringType(), True),
        T.StructField("IsPrimary", T.BooleanType(), True),
        T.StructField("CreatedDate", T.StringType(), True),
        T.StructField("SystemModstamp", T.StringType(), True),
    ])

    # 1. Load Bronze Datasets
    print("Loading Bronze Datasets...")
    b_users = spark.read.parquet(f"{BRONZE_BASE_PATH}/salesforce/user")
    b_accounts = spark.read.parquet(f"{BRONZE_BASE_PATH}/salesforce/account")
    b_contacts = spark.read.parquet(f"{BRONZE_BASE_PATH}/salesforce/contact")
    b_leads = spark.read.parquet(f"{BRONZE_BASE_PATH}/salesforce/lead")
    b_opps = spark.read.parquet(f"{BRONZE_BASE_PATH}/salesforce/opportunity")
    
    # Load OCR with name-variation fallback
    ocr_candidates = [
        f"{BRONZE_BASE_PATH}/salesforce/opportunity_contact_role",
        f"{BRONZE_BASE_PATH}/salesforce/opportunitycontactrole",
        f"{BRONZE_BASE_PATH}/salesforce/opportunity_contact_roles",
    ]
    b_ocrs = load_bronze_table_or_empty(spark, ocr_candidates, ocr_schema)
    
    # Load Customer Events with name-variation fallback
    event_schema = T.StructType([
        T.StructField("event_id", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("identity_resolution", T.StructType([
            T.StructField("anonymous_id", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("domain", T.StringType(), True),
            T.StructField("sfdc_contact_id", T.StringType(), True),
            T.StructField("sfdc_account_id", T.StringType(), True),
        ]), True),
        T.StructField("attributes", T.StructType([
            T.StructField("url", T.StringType(), True),
            T.StructField("plan_interest", T.StringType(), True),
            T.StructField("duration_seconds", T.IntegerType(), True),
        ]), True),
    ])
    event_candidates = [
        f"{BRONZE_BASE_PATH}/events/customer_events",
        f"{BRONZE_BASE_PATH}/events",
        f"{BRONZE_BASE_PATH}/customer_events",
    ]
    b_events = load_bronze_table_or_empty(spark, event_candidates, event_schema)

    # 2. Transform CRM Entities
    print("\nTransforming Salesforce Canonical Entities...")
    s_sales_rep = transform_sales_rep(b_users)
    s_account = transform_account(b_accounts)
    s_contact = transform_contact(b_contacts)
    s_lead = transform_lead(b_leads)
    s_opportunity = transform_opportunity(b_opps)
    s_ocr = transform_opportunity_contact_role(b_ocrs)

    # 3. Identity Resolution
    print("\nExecuting Deterministic Identity Resolution Waterfall...")
    s_identity = build_or_update_identity_map(b_events, s_contact, s_account)

    # 4. Canonicalize & Enrich Events
    print("\nTransforming & Enriching Digital Events...")
    s_customer_event = transform_customer_events(b_events, s_identity)

    # 5. Integrity Validations
    validate_referential_integrity(s_contact, s_account, s_opportunity, s_ocr, s_sales_rep)

    # 6. Write Silver Layer (Delta Lake / Parquet)
    print("\nWriting Silver Tables to Lakehouse...")
    outputs = {
        "sales_rep": s_sales_rep,
        "account": s_account,
        "contact": s_contact,
        "lead": s_lead,
        "opportunity": s_opportunity,
        "opportunity_contact_role": s_ocr,
        "identity": s_identity,
        "customer_event": s_customer_event,
    }

    for name, df in outputs.items():
        out_path = f"{SILVER_BASE_PATH}/{name}"
        df.write.mode("overwrite").parquet(out_path)
        print(f"  -> Successfully written silver.{name} ({df.count()} rows)")


if __name__ == "__main__":
    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("Phase5_Silver_Canonical_Transformations")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )

    run_silver_pipeline(spark_session)
    spark_session.stop()