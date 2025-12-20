from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List
from uuid import uuid4

import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import matplotlib.pyplot as plt
import sqlite3

# Reuse the same event + validation logic conceptually
from services.common.models import OrderPayload, EventEnvelope, make_event

OUTDIR = Path(__file__).parent / "output"
OUTDIR.mkdir(parents=True, exist_ok=True)

DB_PATH = OUTDIR / "orders.db"
LOG_IMG = OUTDIR / "sample_run_logs.png"
TIMELINE_IMG = OUTDIR / "event_timeline.png"

def utc_now():
    return datetime.now(timezone.utc).isoformat()

@dataclass
class Bus:
    handlers: Dict[str, List[Callable[[EventEnvelope], None]]]

    def __init__(self):
        self.handlers = {}

    def subscribe(self, event_type: str, fn: Callable[[EventEnvelope], None]):
        self.handlers.setdefault(event_type, []).append(fn)

    def publish(self, event: EventEnvelope):
        for fn in self.handlers.get(event.event_type, []):
            fn(event)

# --- Storage (SQLite for local demo) ---
def init_sqlite(db_path: Path):
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS orders(
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            items_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_events(
            event_id TEXT PRIMARY KEY,
            processed_at TEXT NOT NULL
        )
        """)
        con.commit()

def already_processed(db_path: Path, event_id: str) -> bool:
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute("SELECT 1 FROM processed_events WHERE event_id=?", (event_id,))
        return cur.fetchone() is not None

def mark_processed(db_path: Path, event_id: str):
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO processed_events(event_id, processed_at) VALUES(?,?)", (event_id, utc_now()))
        con.commit()

def persist_order(db_path: Path, payload: dict):
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO orders(order_id, customer_id, items_json, created_at) VALUES(?,?,?,?)",
            (payload["order_id"], payload["customer_id"], json.dumps(payload["items"]), utc_now()),
        )
        con.commit()

# --- Pipeline handlers ---
logs = []
timeline = []

def log(line: str):
    logs.append(line)

def record(event: EventEnvelope):
    timeline.append((event.occurred_at, event.event_type, event.source, event.trace_id, event.payload.get("order_id")))

def api_create_order(bus: Bus, payload: dict):
    OrderPayload(**payload)  # validates
    event = make_event("order.created", payload, source="api-service")
    record(event)
    log(f"[API] published order.created | order_id={payload['order_id']} trace_id={event.trace_id}")
    bus.publish(event)

def validator_handler(bus: Bus):
    def _handle(event: EventEnvelope):
        try:
            OrderPayload(**event.payload)
            if len(event.payload.get("items", [])) > 20:
                raise ValueError("Too many items (max 20).")
            out = make_event("order.validated", event.payload, source="validator-service", trace_id=event.trace_id)
            record(out)
            log(f"[VALIDATOR] ok -> order.validated | order_id={event.payload['order_id']} trace_id={event.trace_id}")
            bus.publish(out)
        except Exception as e:
            dlq = make_event("order.dlq", event.payload, source="validator-service", trace_id=event.trace_id)
            dlq.error = f"validation_error: {e}"
            record(dlq)
            log(f"[VALIDATOR] DLQ | reason={e} order_id={event.payload.get('order_id')} trace_id={event.trace_id}")
    return _handle

def storage_handler(bus: Bus, db_path: Path):
    def _handle(event: EventEnvelope):
        if already_processed(db_path, event.event_id):
            log(f"[STORAGE] duplicate skipped | event_id={event.event_id}")
            return
        try:
            persist_order(db_path, event.payload)
            mark_processed(db_path, event.event_id)
            out = make_event("order.stored", event.payload, source="storage-service", trace_id=event.trace_id)
            record(out)
            log(f"[STORAGE] stored -> order.stored | order_id={event.payload['order_id']} trace_id={event.trace_id}")
            bus.publish(out)
        except Exception as e:
            dlq = make_event("order.dlq", event.payload, source="storage-service", trace_id=event.trace_id)
            dlq.error = f"storage_error: {e}"
            record(dlq)
            log(f"[STORAGE] DLQ | reason={e} order_id={event.payload.get('order_id')} trace_id={event.trace_id}")
    return _handle

def notifier_handler():
    def _handle(event: EventEnvelope):
        record(event)
        log(f"[NOTIFIER] Notification sent | order_id={event.payload.get('order_id')} trace_id={event.trace_id}")
    return _handle

# --- Outputs ---
def render_logs_image(path: Path):
    import PIL.Image, PIL.ImageDraw, PIL.ImageFont

    # Basic text rendering
    width, height = 1400, 800
    img = PIL.Image.new("RGB", (width, height), (255, 255, 255))
    draw = PIL.ImageDraw.Draw(img)

    try:
        font = PIL.ImageFont.truetype("DejaVuSansMono.ttf", 20)
    except Exception:
        font = PIL.ImageFont.load_default()

    y = 20
    draw.text((20, 10), "EventFlow Sample Run Logs (local simulator)", fill=(0, 0, 0), font=font)
    y = 50
    for line in logs[-28:]:
        draw.text((20, y), line, fill=(0, 0, 0), font=font)
        y += 26
        if y > height - 30:
            break
    img.save(path)

def render_timeline_plot(path: Path):
    # convert timestamps to sequence index
    x = list(range(len(timeline)))
    labels = [t[1] for t in timeline]
    plt.figure(figsize=(12, 4))
    plt.plot(x, [1]*len(x), marker="o")
    plt.yticks([])
    plt.xlabel("Event sequence")
    plt.title("EventFlow: end-to-end event progression (simulated)")
    for i, (idx, lab) in enumerate(zip(x, labels)):
        plt.text(idx, 1.02, lab, rotation=30, ha="right", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()

def main():
    init_sqlite(DB_PATH)

    bus = Bus()
    bus.subscribe("order.created", validator_handler(bus))
    bus.subscribe("order.validated", storage_handler(bus, DB_PATH))
    bus.subscribe("order.stored", notifier_handler())

    # run a clean order
    api_create_order(bus, {"order_id": "A-1001", "customer_id": "C-42", "items": [{"sku": "SKU-1", "qty": 2}, {"sku": "SKU-2", "qty": 1}]})
    # run an invalid order (qty 0) to show DLQ behavior
    try:
        api_create_order(bus, {"order_id": "A-1002", "customer_id": "C-77", "items": [{"sku": "SKU-9", "qty": 0}]})
    except Exception as e:
        log(f"[API] rejected request (client-side validation) | order_id=A-1002 reason={e}")

    render_timeline_plot(TIMELINE_IMG)
    render_logs_image(LOG_IMG)

    print("Wrote:")
    print(" -", DB_PATH)
    print(" -", TIMELINE_IMG)
    print(" -", LOG_IMG)

if __name__ == "__main__":
    main()