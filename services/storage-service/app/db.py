from __future__ import annotations
import os
from sqlalchemy import create_engine, text

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://app:app@postgres:5432/eventflow")

engine = create_engine(POSTGRES_DSN, pool_pre_ping=True)

def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            items_json TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS processed_events (
            event_id TEXT PRIMARY KEY,
            processed_at TIMESTAMPTZ DEFAULT NOW()
        );
        """))
