from __future__ import annotations

import os
import time

from services.common.kafka_utils import build_consumer, wait_for_kafka
from services.common.logging_utils import setup_logger, log_json
from services.common.models import EventEnvelope

logger = setup_logger("notifier-service")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "order.stored")
GROUP_ID = os.getenv("GROUP_ID", "notifier-service-v1")

def main():
    wait_for_kafka(BOOTSTRAP)
    consumer = build_consumer(BOOTSTRAP, IN_TOPIC, GROUP_ID)

    log_json(logger, service="notifier-service", msg="started", kafka=BOOTSTRAP, in_topic=IN_TOPIC)

    while True:
        any_msg = False
        for msg in consumer:
            any_msg = True
            event = EventEnvelope(**msg.value)

            # Serverless-style: triggered by event, does one small job, then exits (here we keep it running).
            log_json(logger, service="notifier-service", action="notify", trace_id=event.trace_id,
                    order_id=event.payload.get("order_id"),
                    message=f"Notification sent for order {event.payload.get('order_id')}")

            consumer.commit()

        if not any_msg:
            time.sleep(0.5)

if __name__ == "__main__":
    main()
