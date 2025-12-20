# Event Schemas

All messages are wrapped in an `EventEnvelope`:

- `event_id`: unique ID (UUID)
- `event_type`: topic-like type string
- `occurred_at`: UTC timestamp
- `trace_id`: correlation ID across services
- `payload`: event body (here: order payload)
- `error`: optional error string for DLQ messages
- `source`: which service produced the event

## Order payload
```json
{
  "order_id": "A-1001",
  "customer_id": "C-42",
  "items": [
    {"sku": "SKU-1", "qty": 2},
    {"sku": "SKU-2", "qty": 1}
  ]
}
```

## Topics
- `order.created`: produced by API
- `order.validated`: produced by validator-service
- `order.stored`: produced by storage-service
- `order.dlq`: produced by validator-service or storage-service when processing fails
