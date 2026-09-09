import pytest

from src.config import RetryConfig
from src.consumer.retry_handler import RetryHandler


class FakeMessage:
    def __init__(self, key=b"1001", value=b"payload", headers=None):
        self._key, self._value, self._headers = key, value, headers

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self):
        return self._headers


class FakeProducer:
    def __init__(self):
        self.produced = []
        self.flushed = 0

    def produce(self, topic, key=None, value=None, headers=None):
        self.produced.append({"topic": topic, "key": key, "value": value, "headers": headers})

    def flush(self, _timeout=None):
        self.flushed += 1


@pytest.fixture
def config():
    return RetryConfig(max_retries=3, initial_delay_seconds=1.0,
                       backoff_multiplier=2.0, max_delay_seconds=30.0)


def test_compute_backoff_is_exponential(config):
    handler = RetryHandler(FakeProducer(), "orders-retry", config)
    assert handler.compute_backoff(1) == 1.0
    assert handler.compute_backoff(2) == 2.0
    assert handler.compute_backoff(3) == 4.0
    assert handler.compute_backoff(4) == 8.0


def test_compute_backoff_is_capped_at_max_delay():
    config = RetryConfig(initial_delay_seconds=1.0, backoff_multiplier=2.0,
                         max_delay_seconds=5.0)
    handler = RetryHandler(FakeProducer(), "orders-retry", config)
    assert handler.compute_backoff(10) == 5.0


def test_should_retry_boundary(config):
    handler = RetryHandler(FakeProducer(), "orders-retry", config)
    assert handler.should_retry(0) is True
    assert handler.should_retry(2) is True
    assert handler.should_retry(3) is False


def test_publish_retry_writes_to_retry_topic_with_headers(config):
    producer = FakeProducer()
    handler = RetryHandler(producer, "orders-retry", config)
    msg = FakeMessage(headers=[("x-fail-times", b"2")])

    delay = handler.publish_retry(msg, new_retry_count=1, error_history="boom")

    assert delay == 1.0
    assert producer.flushed == 1
    assert len(producer.produced) == 1
    sent = producer.produced[0]
    assert sent["topic"] == "orders-retry"
    assert sent["value"] == b"payload"
    headers = dict(sent["headers"])
    assert headers["retry_count"] == b"1"
    assert headers["x-fail-times"] == b"2"          # original headers preserved
    assert headers["x-error-history"] == b"boom"
    assert "not_before" in headers
