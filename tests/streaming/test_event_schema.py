import json
from pathlib import Path
import jsonschema
import pytest

SCHEMA_PATH = Path("kafka/schemas/customer_event.json")

@pytest.fixture
def event_schema():
    with open(SCHEMA_PATH) as f:
        return json.load(f)

def test_valid_anonymous_event(event_schema):
    payload = {
        "event_id": "evt_abc123456789",
        "event_type": "website_visit",
        "timestamp": "2026-08-17T15:30:00Z",
        "identity_resolution": {
            "sfdc_contact_id": None,
            "sfdc_account_id": None,
            "anonymous_id": "anon_998877",
            "email": None,
            "domain": "acme.com",
        },
        "attributes": {
            "url": "https://acme.com/home",
            "duration_seconds": 12,
        },
    }
    jsonschema.validate(instance=payload, schema=event_schema)

def test_valid_known_event(event_schema):
    payload = {
        "event_id": "evt_def987654321",
        "event_type": "demo_request",
        "timestamp": "2026-08-17T15:35:00Z",
        "identity_resolution": {
            "sfdc_contact_id": "0038c00002A1xBCAAZ",
            "sfdc_account_id": "0018c00002A1xACAAZ",
            "anonymous_id": "anon_998877",
            "email": "lead@acme.com",
            "domain": "acme.com",
        },
        "attributes": {
            "url": "https://acme.com/demo",
            "plan_interest": "enterprise",
        },
    }
    jsonschema.validate(instance=payload, schema=event_schema)

def test_invalid_event_type_fails(event_schema):
    payload = {
        "event_id": "evt_1234567890ab",
        "event_type": "unknown_random_action",
        "timestamp": "2026-08-17T15:30:00Z",
        "identity_resolution": {
            "sfdc_contact_id": None,
            "sfdc_account_id": None,
            "anonymous_id": "anon_123",
            "email": None,
            "domain": None,
        },
        "attributes": {},
    }
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=payload, schema=event_schema)

def test_missing_required_field_fails(event_schema):
    payload = {
        "event_type": "website_visit",
        "timestamp": "2026-08-17T15:30:00Z",
        "identity_resolution": {},
        "attributes": {},
    }
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=payload, schema=event_schema)