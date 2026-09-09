"""Kafka consumer for Avro-encoded order messages.

Pipeline per message (from either ``orders`` or ``orders-retry``):

  1. If it is a not-yet-due retry message, wait until ``not_before``.
  2. Avro-decode the value            -> failure => DLQ (permanent).
  3. Build/validate the ``Order``      -> failure => DLQ (permanent).
  4. Classify the processing outcome (``FailureSimulator``):
       SUCCESS   -> update the running-average aggregator.
       TRANSIENT -> re-publish to ``orders-retry`` with backoff, or DLQ once
                    ``retry.max_retries`` is exhausted.
  5. Commit the source offset (manual, synchronous, after terminal handling).

Delivery semantics are at-least-once: the retry/DLQ producer is flushed before
the source offset is committed, so a crash in that window causes reprocessing.
"""

import signal
import time

from confluent_kafka import Consumer, Producer

from src.aggregation import PriceAggregator
from src.config import PROJECT_ROOT, AppConfig
from src.consumer.dlq_handler import DLQHandler
from src.consumer.failure_simulator import FailureSimulator, Outcome, TransientError
from src.consumer.headers import int_header, to_dict
from src.consumer.retry_handler import RetryHandler
from src.models.order import Order
from src.serialization import AvroCodec, AvroDecodeError


class OrderConsumer:
    def __init__(
        self,
        config: AppConfig,
        bootstrap_servers: str = None,
        group_id: str = None,
        transient_rate: float = None,
    ):
        self.config = config
        bootstrap = bootstrap_servers or config.kafka.bootstrap_servers
        self.group_id = group_id or config.kafka.group_id

        self.codec = AvroCodec(PROJECT_ROOT / config.schema.path)
        self.aggregator = PriceAggregator()
        self.simulator = FailureSimulator(
            config.consumer.transient_failure_rate
            if transient_rate is None else transient_rate
        )

        self._consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": self.group_id,
            "auto.offset.reset": config.consumer.auto_offset_reset,
            "enable.auto.commit": config.consumer.enable_auto_commit,
        })
        self._egress = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": "order-consumer-egress",
            "acks": "all",
            "enable.idempotence": True,
        })
        self.retry = RetryHandler(self._egress, config.topics.retry, config.retry)
        self.dlq = DLQHandler(self._egress, config.topics.dlq)

        self._topics = [config.topics.orders, config.topics.retry]
        self._running = False
        self._shut_down = False
        self._ok = 0
        self._retried = 0
        self._dead_lettered = 0

    # -- lifecycle ---------------------------------------------------------
    def run(self) -> None:
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)
        self._consumer.subscribe(self._topics)
        self._running = True
        print(f"Consuming {self._topics} as group '{self.group_id}'. Ctrl-C to stop.")

        try:
            while self._running:
                msg = self._consumer.poll(self.config.consumer.poll_timeout_seconds)
                if msg is None:
                    continue
                if msg.error():
                    print(f"[KAFKA-ERR] {msg.error()}")
                    continue
                try:
                    self._handle(msg)
                except Exception as exc:  # last-resort safety net: never poison-loop
                    print(f"[ERROR] unexpected failure, routing to DLQ: {exc!r}")
                    retry_count = int_header(to_dict(msg.headers()), "retry_count", 0)
                    self._to_dlq(msg, "unexpected processing error", exc, retry_count)
                self._consumer.commit(msg, asynchronous=False)
        finally:
            self._shutdown()

    def _on_signal(self, _signum, _frame) -> None:
        self._running = False

    def _shutdown(self) -> None:
        if self._shut_down:
            return
        self._shut_down = True
        print("\nShutting down...")
        try:
            self._egress.flush(10)
        finally:
            self._consumer.close()
        self._print_summary()

    # -- per-message handling -------------------------------------------------
    def _handle(self, msg) -> None:
        headers = to_dict(msg.headers())
        retry_count = int_header(headers, "retry_count", 0)
        source = msg.topic()

        self._await_retry_delay(headers)

        # 1. decode
        try:
            record = self.codec.decode(msg.value())
        except AvroDecodeError as exc:
            self._to_dlq(msg, "avro decode failed", exc, retry_count)
            return

        # 2. validate domain object
        try:
            order = Order.from_avro_dict(record)
        except ValueError as exc:
            self._to_dlq(msg, "order validation failed", exc, retry_count)
            return

        # 3. process
        outcome = self.simulator.classify(headers, retry_count)
        if outcome == Outcome.SUCCESS:
            avg = self.aggregator.add(order.price, order.product)
            self._ok += 1
            print(f"[OK]  {order}  (from {source}, attempt {retry_count + 1})")
            print(f"[AGG] processed={self.aggregator.count} "
                  f"running_avg=${avg:.2f} last=${order.price:.2f}")
            return

        # 4. transient failure -> retry topic or DLQ
        note = f"transient failure on attempt {retry_count + 1}"
        if self.retry.should_retry(retry_count):
            new_count = retry_count + 1
            history = "; ".join(h for h in (headers.get("x-error-history", ""), note) if h)
            delay = self.retry.publish_retry(msg, new_count, history)
            self._retried += 1
            print(f"[RETRY] {order.order_id} attempt={new_count} "
                  f"backoff={delay:.1f}s -> {self.config.topics.retry}")
        else:
            self._to_dlq(msg, "max retries exceeded", TransientError(note), retry_count)

    def _await_retry_delay(self, headers: dict) -> None:
        not_before_ms = int_header(headers, "not_before", 0)
        if not not_before_ms:
            return
        wait = not_before_ms / 1000.0 - time.time()
        if wait > 0:
            time.sleep(min(wait, self.config.retry.max_delay_seconds))

    def _to_dlq(self, msg, reason: str, error: Exception, retry_count: int) -> None:
        self.dlq.send(msg, reason, error, retry_count)
        self._dead_lettered += 1
        key = msg.key().decode() if msg.key() else "<none>"
        print(f"[DLQ] {key} reason='{reason}' detail='{error}' retry_count={retry_count}")

    # -- reporting -------------------------------------------------------------
    def _print_summary(self) -> None:
        s = self.aggregator.snapshot()
        lo = f"${s['min_price']:.2f}" if s["min_price"] is not None else "-"
        hi = f"${s['max_price']:.2f}" if s["max_price"] is not None else "-"
        per_product = ", ".join(
            f"{name}=${avg:.2f}" for name, avg in s["per_product_average"].items()
        ) or "-"
        print("=== SUMMARY ===")
        print(f"  processed (OK)  : {self._ok}")
        print(f"  retries issued  : {self._retried}")
        print(f"  sent to DLQ     : {self._dead_lettered}")
        print(f"  running average : ${s['running_average']:.2f}")
        print(f"  min / max price : {lo} / {hi}")
        print(f"  per-product avg : {per_product}")
