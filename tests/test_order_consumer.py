"""Integration-ish tests for the consumer pipeline.

The Kafka ``Consumer`` / ``Producer`` are replaced with fakes so the full
decode -> validate -> classify -> (aggregate | retry | DLQ) path can be driven
in-process without a broker.
"""

import pytest

import src.consumer.order_consumer as oc
from src.config import load_config
from src.serialization import AvroCodec
from src.config import PROJECT_ROOT

CODEC = AvroCodec(PROJECT_ROOT / "schemas" / "order.avsc")


class FakeKafkaConsumer:
    def __init__(self, *_a, **_k):
        pass

    def subscribe(self, *_a, **_k):
        pass

    def commit(self, *_a, **_k):
        pass

    def close(self):
        pass


class FakeProducer:
    def __init__(self, *_a, **_k):
        self.produced = []

    def produce(self, topic, key=None, value=None, headers=None, **_k):
        self.produced.append({"topic": topic, "key": key, "value": value,
                              "headers": dict(headers or [])})

    def flush(self, *_a, **_k):
        return 0

    def poll(self, *_a, **_k):
        return 0


class FakeMessage:
    def __init__(self, value, headers=None, topic="orders", partition=0, offset=0, key=b"1001"):
        self._value, self._headers = value, headers
        self._topic, self._partition, self._offset, self._key = topic, partition, offset, key

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key


@pytest.fixture
def consumer(monkeypatch):
    monkeypatch.setattr(oc, "Consumer", FakeKafkaConsumer)
    monkeypatch.setattr(oc, "Producer", FakeProducer)
    c = oc.OrderConsumer(load_config(), transient_rate=0.0)
    return c


def _order_bytes(order_id="1001", product="Laptop", price=100.0):
    return CODEC.encode({"orderId": order_id, "product": product, "price": price})


def test_valid_order_updates_aggregator(consumer):
    consumer._handle(FakeMessage(_order_bytes(price=100.0)))
    consumer._handle(FakeMessage(_order_bytes(price=200.0)))
    assert consumer._ok == 2
    assert consumer.aggregator.running_average == 150.0
    assert consumer._egress.produced == []  # nothing sent to retry/DLQ


def test_undecodable_value_goes_to_dlq(consumer):
    consumer._handle(FakeMessage(b"\x00\xff\x00garbage"))
    assert consumer._dead_lettered == 1
    sent = consumer._egress.produced[-1]
    assert sent["topic"] == "orders-dlq"
    assert sent["headers"]["x-error-reason"] == b"avro decode failed"


def test_invalid_order_goes_to_dlq(consumer):
    consumer._handle(FakeMessage(_order_bytes(price=-5.0)))
    assert consumer._dead_lettered == 1
    sent = consumer._egress.produced[-1]
    assert sent["topic"] == "orders-dlq"
    assert sent["headers"]["x-error-reason"] == b"order validation failed"


def test_transient_failure_is_republished_to_retry_topic(consumer):
    msg = FakeMessage(_order_bytes(), headers=[("x-fail-times", b"2")])
    consumer._handle(msg)
    assert consumer._retried == 1
    assert consumer._ok == 0
    sent = consumer._egress.produced[-1]
    assert sent["topic"] == "orders-retry"
    assert sent["headers"]["retry_count"] == b"1"
    assert sent["headers"]["x-fail-times"] == b"2"


def test_retry_exhaustion_goes_to_dlq(consumer):
    # retry_count already at max_retries -> no further retry
    msg = FakeMessage(
        _order_bytes(),
        headers=[("x-fail-times", b"9"), ("retry_count", b"3")],
        topic="orders-retry",
    )
    consumer._handle(msg)
    assert consumer._dead_lettered == 1
    sent = consumer._egress.produced[-1]
    assert sent["topic"] == "orders-dlq"
    assert sent["headers"]["x-error-reason"] == b"max retries exceeded"
    assert sent["headers"]["x-retry-count"] == b"3"


def test_flaky_message_eventually_succeeds(consumer):
    """Simulate the retry loop: fail twice, then succeed on the 3rd attempt."""
    for attempt in range(3):
        msg = FakeMessage(
            _order_bytes(price=90.0),
            headers=[("x-fail-times", b"2"), ("retry_count", str(attempt).encode())],
            topic="orders" if attempt == 0 else "orders-retry",
        )
        consumer._handle(msg)
    assert consumer._retried == 2
    assert consumer._ok == 1
    assert consumer.aggregator.running_average == 90.0
