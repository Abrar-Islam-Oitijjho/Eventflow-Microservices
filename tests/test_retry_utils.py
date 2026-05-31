import pytest

from services.common.retry_utils import retry_with_backoff


def test_retry_with_backoff_succeeds_after_failure():
    attempts = {"count": 0}

    def flaky_operation():
        attempts["count"] += 1

        if attempts["count"] < 2:
            raise RuntimeError("Temporary failure")

        return "success"

    result = retry_with_backoff(
        flaky_operation,
        attempts=3,
        base_delay_s=0.01,
        max_delay_s=0.01,
    )

    assert result == "success"
    assert attempts["count"] == 2


def test_retry_with_backoff_fails_after_max_attempts():
    attempts = {"count": 0}

    def failing_operation():
        attempts["count"] += 1
        raise RuntimeError("Permanent failure")

    with pytest.raises(RuntimeError, match="Operation failed after 3 attempts"):
        retry_with_backoff(
            failing_operation,
            attempts=3,
            base_delay_s=0.01,
            max_delay_s=0.01,
        )

    assert attempts["count"] == 3