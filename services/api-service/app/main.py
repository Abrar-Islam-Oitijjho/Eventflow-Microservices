from __future__ import annotations

import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from services.common.kafka_utils import build_producer, wait_for_kafka
from services.common.logging_utils import setup_logger, log_json
from services.common.models import OrderPayload, make_event

app = FastAPI(title="EventFlow API", version="1.0.0")
logger = setup_logger("api-service")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.getenv("CREATED_TOPIC", "order.created")

producer = None

@app.on_event("startup")
def on_startup():
    global producer
    wait_for_kafka(BOOTSTRAP)
    producer = build_producer(BOOTSTRAP)
    log_json(logger, service="api-service", msg="started", kafka=BOOTSTRAP, topic=TOPIC)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders")
def create_order(order: OrderPayload):
    if producer is None:
        raise HTTPException(status_code=503, detail="Producer not ready")

    event = make_event("order.created", order.model_dump(), source="api-service")
    producer.send(TOPIC, value=event.model_dump())
    producer.flush(timeout=10)

    log_json(logger, service="api-service", action="publish", topic=TOPIC, event_type=event.event_type,
             event_id=event.event_id, trace_id=event.trace_id, order_id=order.order_id)

    return JSONResponse(
        status_code=202,
        content={"accepted": True, "event_id": event.event_id, "trace_id": event.trace_id, "topic": TOPIC},
    )
