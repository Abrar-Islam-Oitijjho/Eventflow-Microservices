from services.common.models import make_event, make_dlq_event


def test_make_dlq_event_preserves_original_event_metadata():
    original_event = make_event(
        "order.validated",
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "amount": 120.5,
        },
        source="validator-service",
        trace_id="trace-123",
    )

    dlq_event = make_dlq_event(
        original_event,
        source="storage-service",
        failure_stage="database_persistence",
        error_message="Database connection failed",
        retry_attempts=3,
    )

    assert dlq_event.event_type == "order.dlq"
    assert dlq_event.source == "storage-service"
    assert dlq_event.trace_id == "trace-123"

    assert dlq_event.payload["original_event_id"] == original_event.event_id
    assert dlq_event.payload["original_event_type"] == "order.validated"
    assert dlq_event.payload["source_service"] == "storage-service"
    assert dlq_event.payload["failure_stage"] == "database_persistence"
    assert dlq_event.payload["error_message"] == "Database connection failed"
    assert dlq_event.payload["retry_attempts"] == 3
    assert dlq_event.payload["original_payload"]["order_id"] == "ORD-001"