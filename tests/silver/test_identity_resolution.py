"""Unit tests verifying the 5-Tier waterfall and retroactive anonymous stitching."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import types as T
from spark.silver.identity.resolve_identity import build_or_update_identity_map


def test_anonymous_to_known_stitching(spark):
    events_schema = T.StructType(
        [
            T.StructField(
                "identity_resolution",
                T.StructType(
                    [
                        T.StructField("anonymous_id", T.StringType(), True),
                        T.StructField("email", T.StringType(), True),
                        T.StructField("domain", T.StringType(), True),
                        T.StructField("sfdc_contact_id", T.StringType(), True),
                        T.StructField("sfdc_account_id", T.StringType(), True),
                    ]
                ),
                True,
            ),
            T.StructField("timestamp", T.StringType(), True),
        ]
    )

    events_data = [
        {
            "identity_resolution": {
                "anonymous_id": "anon_123",
                "email": None,
                "domain": None,
                "sfdc_contact_id": None,
                "sfdc_account_id": None,
            },
            "timestamp": "2026-02-01T10:00:00Z",
        },
        {
            "identity_resolution": {
                "anonymous_id": "anon_123",
                "email": "sarah@acme.com",
                "domain": "acme.com",
                "sfdc_contact_id": None,
                "sfdc_account_id": None,
            },
            "timestamp": "2026-02-01T10:05:00Z",
        },
    ]

    contacts_data = [
        {
            "contact_id": "cnt_003999",
            "account_id": "acc_001888",
            "email": "sarah@acme.com",
            "source_record_id": "003999",
            "updated_at": "2026-01-01T00:00:00Z",
        }
    ]

    accounts_data = [
        {
            "account_id": "acc_001888",
            "website": "acme.com",
            "source_record_id": "001888",
            "updated_at": "2026-01-01T00:00:00Z",
        }
    ]

    events_df = spark.createDataFrame(events_data, schema=events_schema)
    contacts_df = spark.createDataFrame(contacts_data)
    accounts_df = spark.createDataFrame(accounts_data)

    identity_map = build_or_update_identity_map(events_df, contacts_df, accounts_df)
    anon_record = identity_map.filter(identity_map.anonymous_id == "anon_123").first()

    assert anon_record["contact_id"] == "cnt_003999"
    assert anon_record["account_id"] == "acc_001888"