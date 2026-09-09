"""Retry-topic handler with exponential backoff.

When the consumer hits a transient failure it does not block the partition
retrying in place. Instead it re-publishes the message to the ``orders-retry``
topic with an incremented ``retry_count`` and a ``not_before`` timestamp. The
same consumer group also subscribes to ``orders-retry``; when it picks the
message back up it waits until ``not_before`` before reprocessing.

Backoff for attempt *n* (1-based):

    delay = initial_delay * (multiplier ** (n - 1)),  capped at max_delay
"""

import time

from src.config import RetryConfig
from src.consumer.headers import merge


class RetryHandler:
    def __init__(self, producer, retry_topic: str, config: RetryConfig):
        self._producer = producer
        self._retry_topic = retry_topic
        self._config = config

    def compute_backoff(self, attempt: int) -> float:
        """Seconds to wait before the given 1-based retry attempt."""
        if attempt < 1:
            return 0.0
        delay = self._config.initial_delay_seconds * (
            self._config.backoff_multiplier ** (attempt - 1)
        )
        return min(delay, self._config.max_delay_seconds)

    def should_retry(self, retry_count: int) -> bool:
        """True if a message that has already been retried ``retry_count`` times
        is still allowed another attempt."""
        return retry_count < self._config.max_retries

    def publish_retry(self, msg, new_retry_count: int, error_history: str) -> float:
        """Re-publish ``msg`` to the retry topic. Returns the backoff delay applied."""
        delay = self.compute_backoff(new_retry_count)
        not_before_ms = int((time.time() + delay) * 1000)
        headers = merge(msg.headers(), {
            "retry_count": new_retry_count,
            "not_before": not_before_ms,
            "x-error-history": error_history,
        })
        self._producer.produce(
            topic=self._retry_topic,
            key=msg.key(),
            value=msg.value(),
            headers=headers,
        )
        self._producer.flush(10)
        return delay
