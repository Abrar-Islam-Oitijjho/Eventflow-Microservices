from __future__ import annotations

import os
import time

from services.common.kafka_utils import build_consumer, build_producer, wait_for_kafka
from services.common.logging_utils import setup_logger, log_json
from services.common.models import EventEnvelope, OrderPayload, make_event

logger = setup_logger("validator-service")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "order.created")
OUT_TOPIC = os.getenv("OUT_TOPIC", "order.validated")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "order.dlq")

GROUP_ID = os.getenv("GROUP_ID", "validator-service-v1")

def validate_order(payload: dict) -> None:
    # pydantic validation
    OrderPayload(**payload)
    # extra rule: max 20 items
    if len(payload.get("items", [])) > 20:
        raise ValueError("Too many items (max 20).")


def main():
    wait_for_kafka(BOOTSTRAP)
    consumer = build_consumer(BOOTSTRAP, IN_TOPIC, GROUP_ID)
    producer = build_producer(BOOTSTRAP)

    log_json(logger, service="validator-service", msg="started", kafka=BOOTSTRAP, in_topic=IN_TOPIC, out_topic=OUT_TOPIC)

    while True:
        any_msg = False
        for msg in consumer:
            any_msg = True
            event = EventEnvelope(**msg.value)
            try:
                validate_order(event.payload)
                out_event = make_event("order.validated", event.payload, source="validator-service", trace_id=event.trace_id)
                producer.send(OUT_TOPIC, value=out_event.model_dump())
                producer.flush(timeout=10)
                consumer.commit()

                log_json(logger, service="validator-service", action="validated", in_event_id=event.event_id,
                        out_event_id=out_event.event_id, trace_id=event.trace_id, order_id=event.payload.get("order_id"))
            except Exception as e:
                dlq = make_event("order.dlq", event.payload, source="validator-service", trace_id=event.trace_id)
                dlq.error = f"validation_error: {e}"
                producer.send(DLQ_TOPIC, value=dlq.model_dump())
                producer.flush(timeout=10)
                consumer.commit()

                log_json(logger, service="validator-service", action="dlq", reason=str(e),
                        trace_id=event.trace_id, order_id=event.payload.get("order_id"))

        if not any_msg:
            time.sleep(0.5)


if __name__ == "__main__":
    main()
