from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, conint, constr


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Item(BaseModel):
    sku: constr(strip_whitespace=True, min_length=1)
    qty: conint(ge=1, le=1000)


class OrderPayload(BaseModel):
    order_id: constr(strip_whitespace=True, min_length=1)
    customer_id: constr(strip_whitespace=True, min_length=1)
    items: List[Item]


class EventEnvelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: Literal["order.created", "order.validated", "order.stored", "order.dlq"]
    occurred_at: datetime = Field(default_factory=utc_now)
    trace_id: str = Field(default_factory=lambda: str(uuid4()))
    payload: dict
    error: Optional[str] = None
    source: Optional[str] = None


def make_event(event_type: str, payload: dict, *, source: str, trace_id: str | None = None) -> EventEnvelope:
    return EventEnvelope(
        event_type=event_type, payload=payload, source=source, trace_id=trace_id or str(uuid4())
    )
