"""Kafka producer for Avro-encoded order messages.

Generates randomized orders and can optionally inject two kinds of bad messages
so the consumer's fault-tolerance can be demonstrated live:

* **poison** messages -- structurally decodable but invalid (negative price or
  empty product). The consumer sends these straight to the DLQ.
* **flaky** messages -- valid, but tagged with an ``x-fail-times`` header telling
  the consumer to fail them transiently N times before succeeding. These travel
  through the retry topic.
"""

import random
import time
from dataclasses import dataclass

from confluent_kafka import Producer

from src.config import PROJECT_ROOT, AppConfig
from src.serialization import AvroCodec


@dataclass
class ProduceStats:
    sent: int = 0
    poison: int = 0
    flaky: int = 0


class OrderProducer:
    def __init__(self, config: AppConfig, bootstrap_servers: str = None):
        self.config = config
        self.topic = config.topics.orders
        self.codec = AvroCodec(PROJECT_ROOT / config.schema.path)
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers or config.kafka.bootstrap_servers,
            "client.id": "order-producer",
            "acks": "all",
            "enable.idempotence": True,
        })
        self._stats = ProduceStats()

    # -- delivery reporting -------------------------------------------------
    @staticmethod
    def _on_delivery(err, msg):
        if err is not None:
            print(f"[SEND-ERR] {err}")
        else:
            print(
                f"[SENT] key={msg.key().decode()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    # -- message construction --------------------------------------------------
    def _build_record(self, order_id: str, poison: bool) -> dict:
        product = random.choice(self.config.producer.products)
        pr = self.config.producer.price_range
        price = round(random.uniform(pr.min, pr.max), 2)

        if poison:
            # Two flavours of invalid data; both encode fine but fail on consume.
            if random.random() < 0.5:
                price = -price
            else:
                product = ""
        return {"orderId": order_id, "product": product, "price": price}

    def _flaky_headers(self) -> list:
        fail_times = random.randint(1, self.config.retry.max_retries)
        return [("x-fail-times", str(fail_times).encode())]

    # -- main loop -----------------------------------------------------------
    def run(
        self,
        count: int,
        interval: float = None,
        poison_rate: float = None,
        flaky_rate: float = None,
    ) -> ProduceStats:
        pcfg = self.config.producer
        interval = pcfg.message_interval_seconds if interval is None else interval
        poison_rate = pcfg.poison_rate if poison_rate is None else poison_rate
        flaky_rate = pcfg.flaky_rate if flaky_rate is None else flaky_rate

        print(
            f"Producing {count} orders to '{self.topic}' "
            f"(interval={interval}s, poison_rate={poison_rate}, flaky_rate={flaky_rate})"
        )

        for i in range(count):
            order_id = str(pcfg.start_order_id + i)
            is_poison = random.random() < poison_rate
            is_flaky = (not is_poison) and random.random() < flaky_rate

            record = self._build_record(order_id, is_poison)
            headers = self._flaky_headers() if is_flaky else None

            self._producer.produce(
                topic=self.topic,
                key=order_id.encode(),
                value=self.codec.encode(record),
                headers=headers,
                on_delivery=self._on_delivery,
            )
            self._stats.sent += 1
            self._stats.poison += int(is_poison)
            self._stats.flaky += int(is_flaky)

            tag = " [poison]" if is_poison else (" [flaky]" if is_flaky else "")
            print(f"  -> order {order_id}: {record['product'] or '<empty>'} "
                  f"${record['price']:.2f}{tag}")

            self._producer.poll(0)
            if interval:
                time.sleep(interval)

        self._producer.flush(15)
        print(
            f"Done. sent={self._stats.sent} poison={self._stats.poison} "
            f"flaky={self._stats.flaky}"
        )
        return self._stats
