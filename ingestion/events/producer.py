import json
import time
import uuid
from datetime import datetime, timezone
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import jsonschema
from confluent_kafka import Producer
from faker import Faker
from ingestion.utils.logger import logger

fake = Faker()

SCHEMA_PATH = Path("kafka/schemas/customer_event.json")
with open(SCHEMA_PATH) as f:
    EVENT_SCHEMA = json.load(f)

KAFKA_CONF = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "b2b-event-generator",
}

producer = Producer(KAFKA_CONF)
TOPIC_NAME = "customer-events"

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def generate_session():
    """Simulates a cohesive buyer journey across events."""
    domain = fake.domain_name()
    email = fake.company_email() if fake.boolean(chance_of_getting_true=60) else None
    anon_id = f"anon_{uuid.uuid4().hex[:10]}"
    sfdc_acc_id = f"001{uuid.uuid4().hex[:15]}" if email else None
    sfdc_con_id = f"003{uuid.uuid4().hex[:15]}" if email else None

    journey_types = ["website_visit", "product_page_view", "pricing_page_view", "demo_request"]
    
    for event_type in journey_types:
        event = {
            "event_id": f"evt_{uuid.uuid4().hex[:12]}",
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "identity_resolution": {
                "sfdc_contact_id": sfdc_con_id,
                "sfdc_account_id": sfdc_acc_id,
                "anonymous_id": anon_id,
                "email": email,
                "domain": domain,
            },
            "attributes": {
                "url": f"https://{domain}/{event_type.replace('_', '-')}",
                "duration_seconds": fake.random_int(min=5, max=180),
                "plan_interest": "enterprise" if event_type == "pricing_page_view" else None,
            },
        }

        # Validate contract before publishing
        try:
            jsonschema.validate(instance=event, schema=EVENT_SCHEMA)
            producer.produce(
                topic=TOPIC_NAME,
                key=anon_id.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            logger.info(f"Published event: {event['event_id']} | Type: {event_type}")
        except jsonschema.ValidationError as e:
            logger.error(f"Event failed contract validation: {e.message}")

        producer.poll(0)
        time.sleep(0.5)

def run_producer(iterations: int = 10):
    logger.info("Starting Kafka Event Producer...")
    for _ in range(iterations):
        generate_session()
    producer.flush()
    logger.info("Finished producing event batches.")

if __name__ == "__main__":
    run_producer()