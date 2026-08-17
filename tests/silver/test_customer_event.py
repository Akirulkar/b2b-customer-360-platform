"""Unit tests verifying digital event canonicalization and identity mapping joins."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import types as T
from spark.silver.events.customer_event import transform_customer_events


def test_customer_event_enrichment(spark):
    events_schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), True),
            T.StructField("event_type", T.StringType(), True),
            T.StructField("timestamp", T.StringType(), True),
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
            T.StructField(
                "attributes",
                T.StructType(
                    [
                        T.StructField("url", T.StringType(), True),
                        T.StructField("plan_interest", T.StringType(), True),
                        T.StructField("duration_seconds", T.IntegerType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    events_data = [
        {
            "event_id": "evt_101",
            "event_type": "pricing_page_view",
            "timestamp": "2026-03-01T12:00:00Z",
            "identity_resolution": {
                "anonymous_id": "anon_abc",
                "email": "user@acme.com",
                "domain": "acme.com",
                "sfdc_contact_id": None,
                "sfdc_account_id": None,
            },
            "attributes": {
                "url": "https://acme.com/pricing",
                "plan_interest": "enterprise",
                "duration_seconds": 45,
            },
        }
    ]

    identity_data = [
        {
            "identity_id": "idn_001",
            "anonymous_id": "anon_abc",
            "email": "user@acme.com",
            "domain": "acme.com",
            "contact_id": "cnt_003111",
            "account_id": "acc_001222",
            "first_seen_at": "2026-03-01T12:00:00Z",
            "last_seen_at": "2026-03-01T12:00:00Z",
            "updated_at": "2026-03-01T12:00:00Z",
        }
    ]

    events_df = spark.createDataFrame(events_data, schema=events_schema)
    identity_df = spark.createDataFrame(identity_data)

    silver_events = transform_customer_events(events_df, identity_df)
    row = silver_events.first()

    assert row["event_id"] == "evt_101"
    assert row["interaction_weight"] == "high"
    assert row["contact_id"] == "cnt_003111"
    assert row["account_id"] == "acc_001222"
    assert row["source_system"] == "digital_events"
    assert row["record_hash"] is not None