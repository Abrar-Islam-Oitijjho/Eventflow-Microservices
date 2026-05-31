from __future__ import annotations
from services.common.retry_utils import retry_with_backoff

import json
import os
import time
from sqlalchemy import text

from services.common.kafka_utils import build_consumer, build_producer, wait_for_kafka
from services.common.logging_utils import setup_logger, log_json
from services.common.models import EventEnvelope, make_event, make_dlq_event
from .db import engine, init_db

logger = setup_logger("storage-service")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "order.validated")
OUT_TOPIC = os.getenv("OUT_TOPIC", "order.stored")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "order.dlq")
GROUP_ID = os.getenv("GROUP_ID", "storage-service-v1")

def already_processed(event_id: str) -> bool:
    with engine.begin() as conn:
        res = conn.execute(text("SELECT 1 FROM processed_events WHERE event_id = :eid"), {"eid": event_id}).fetchone()
        return res is not None

def mark_processed(event_id: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO processed_events(event_id) VALUES(:eid) ON CONFLICT DO NOTHING"), {"eid": event_id})

def persist_order(payload: dict) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO orders(order_id, customer_id, items_json) VALUES(:oid,:cid,:items) "
                 "ON CONFLICT (order_id) DO NOTHING"),
            {"oid": payload["order_id"], "cid": payload["customer_id"], "items": json.dumps(payload["items"])},
        )

def main():
    init_db()
    wait_for_kafka(BOOTSTRAP)
    consumer = build_consumer(BOOTSTRAP, IN_TOPIC, GROUP_ID)
    producer = build_producer(BOOTSTRAP)

    log_json(logger, service="storage-service", msg="started", kafka=BOOTSTRAP, in_topic=IN_TOPIC, out_topic=OUT_TOPIC)

    while True:
        any_msg = False
        for msg in consumer:
            any_msg = True
            event = EventEnvelope(**msg.value)

            if already_processed(event.event_id):
                consumer.commit()
                log_json(logger, service="storage-service", action="skip_duplicate", event_id=event.event_id, trace_id=event.trace_id)
                continue

            try:
                retry_with_backoff(lambda: persist_order(event.payload), attempts=3)
                retry_with_backoff(lambda: mark_processed(event.event_id), attempts=3)

                out_event = make_event("order.stored", event.payload, source="storage-service", trace_id=event.trace_id)
                producer.send(OUT_TOPIC, value=out_event.model_dump())
                producer.flush(timeout=10)

                consumer.commit()
                log_json(logger, service="storage-service", action="stored", in_event_id=event.event_id,
                        out_event_id=out_event.event_id, trace_id=event.trace_id, order_id=event.payload.get("order_id"))
            except Exception as e:
                dlq = make_dlq_event(
                    event,
                    source="storage-service",
                    failure_stage="database_persistence",
                    error_message=str(e),
                    retry_attempts=3,
                )

                producer.send(DLQ_TOPIC, value=dlq.model_dump())
                producer.flush(timeout=10)
                consumer.commit()

                log_json(
                    logger,
                    service="storage-service",
                    action="dlq",
                    reason=str(e),
                    failure_stage="database_persistence",
                    retry_attempts=3,
                    trace_id=event.trace_id,
                    order_id=event.payload.get("order_id"),
                )

        if not any_msg:
            time.sleep(0.5)

if __name__ == "__main__":
    main()
