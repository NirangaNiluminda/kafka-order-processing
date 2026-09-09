"""Dead Letter Queue handler.

Publishes permanently failed messages to ``orders-dlq``. The original Avro value
is preserved unchanged so a DLQ consumer can still decode the order; all failure
context is attached as headers:

    x-original-topic / x-original-partition / x-original-offset / x-original-key
    x-error-reason   -- short category ("order validation failed", ...)
    x-error-class    -- exception type name
    x-error-detail   -- exception message (truncated)
    x-failed-at      -- ISO-8601 UTC timestamp
    x-retry-count    -- attempts made before giving up
"""

from datetime import datetime, timezone

from src.consumer.headers import merge

_DETAIL_LIMIT = 1024


class DLQHandler:
    def __init__(self, producer, dlq_topic: str):
        self._producer = producer
        self._dlq_topic = dlq_topic

    def send(self, msg, reason: str, error: Exception = None, retry_count: int = 0) -> None:
        detail = "" if error is None else str(error)[:_DETAIL_LIMIT]
        overrides = {
            "x-original-topic": msg.topic() or "",
            "x-original-partition": msg.partition(),
            "x-original-offset": msg.offset(),
            "x-original-key": msg.key() or b"",
            "x-error-reason": reason,
            "x-error-class": type(error).__name__ if error else "",
            "x-error-detail": detail,
            "x-failed-at": datetime.now(timezone.utc).isoformat(),
            "x-retry-count": retry_count,
        }
        self._producer.produce(
            topic=self._dlq_topic,
            key=msg.key(),
            value=msg.value(),
            headers=merge(msg.headers(), overrides),
        )
        self._producer.flush(10)
