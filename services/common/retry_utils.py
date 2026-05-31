from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


def retry_with_backoff(
    operation: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay_s: float = 0.5,
    max_delay_s: float = 5.0,
) -> T:
    """
    Retry an operation with exponential backoff.

    Example delays:
    attempt 1 fails -> wait 0.5s
    attempt 2 fails -> wait 1.0s
    attempt 3 fails -> raise error
    """
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            last_error = exc

            if attempt == attempts:
                break

            delay = min(base_delay_s * (2 ** (attempt - 1)), max_delay_s)
            time.sleep(delay)

    raise RuntimeError(f"Operation failed after {attempts} attempts") from last_error