from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone


def setup_logger(name: str) -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_json(logger: logging.Logger, **fields):
    fields.setdefault("ts", datetime.now(timezone.utc).isoformat())
    logger.info(json.dumps(fields, default=str))
