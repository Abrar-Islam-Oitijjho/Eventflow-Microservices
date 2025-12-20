from __future__ import annotations

import json
import os
import time
from typing import Callable, Optional

from kafka import KafkaConsumer, KafkaProducer

from .models import EventEnvelope


def _json_serializer(v):
    return json.dumps(v, default=str).encode("utf-8")


def build_producer(bootstrap: str) -> KafkaProducer:
    # acks='all' is safer; linger_ms small for demo
    return KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=_json_serializer,
        acks="all",
        linger_ms=5,
        retries=5,
    )


def build_consumer(bootstrap: str, topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap],
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
    )


def wait_for_kafka(bootstrap: str, timeout_s: int = 60) -> None:
    t0 = time.time()
    last_err: Optional[Exception] = None
    while time.time() - t0 < timeout_s:
        try:
            p = build_producer(bootstrap)
            p.close()
            return
        except Exception as e:
            last_err = e
            time.sleep(1)
    raise RuntimeError(f"Kafka not ready at {bootstrap}. Last error: {last_err}")
