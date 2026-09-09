# Live Demo Script

A ~5 minute walkthrough that exercises every assignment requirement: Avro
messaging, real-time aggregation, retry-on-transient-failure, and the Dead Letter
Queue.

## 0. One-time setup

```bash
make install          # venv + dependencies
```

## 1. Start Kafka and create the topics

```bash
make up               # docker compose up -d, then shows kafka-init logs
```

Wait until `kafka-init` prints:

```
orders
orders-dlq
orders-retry
```

*(Optional)* start the web UI and open <http://localhost:8080>:

```bash
make ui
```

## 2. Start the consumer

In **terminal A**:

```bash
make consume          # == venv/bin/python run_consumer.py
```

It subscribes to `orders` **and** `orders-retry` and waits.

## 3. Produce a clean batch — show Avro + aggregation

In **terminal B**:

```bash
venv/bin/python run_producer.py --count 20 --interval 0.3
```

Terminal A shows, after every message:

```
[OK]  Order(id=1003, product=Monitor, price=$312.40)  (from orders, attempt 1)
[AGG] processed=3 running_avg=$254.71 last=$312.40
```

Talking points:
- values on the wire are Avro binary (`schemaless_writer`, schema `schemas/order.avsc`)
- `running_avg` updates in real time as each order is processed

## 4. Inject transient failures — show the retry topic

```bash
venv/bin/python run_producer.py --count 20 --interval 0.3 --flaky-rate 0.4
```

Flaky messages carry an `x-fail-times` header. Terminal A shows the retry loop:

```
[RETRY] 1027 attempt=1 backoff=1.0s -> orders-retry
[RETRY] 1027 attempt=2 backoff=2.0s -> orders-retry
[OK]  Order(id=1027, product=Keyboard, price=$88.10)  (from orders-retry, attempt 3)
[AGG] processed=31 running_avg=$261.05 last=$88.10
```

The message bounces through `orders-retry` with exponential backoff (1s, 2s, 4s)
until it succeeds. Point out it came back `from orders-retry`.

## 5. Inject poison messages — show the DLQ

```bash
venv/bin/python run_producer.py --count 20 --interval 0.3 --poison-rate 0.3
```

Poison messages have a negative price or empty product. Terminal A:

```
[DLQ] 1052 reason='order validation failed' detail='price must be non-negative, got -145.7' retry_count=0
```

A message that stays transient past `max_retries` also lands in the DLQ:

```
[DLQ] 1058 reason='max retries exceeded' detail='transient failure on attempt 4' retry_count=3
```

## 6. Inspect the Dead Letter Queue

In **terminal C**:

```bash
make dlq
```

Each record keeps its original Avro value and carries failure-metadata headers
(printed as one comma-separated line, then a tab, then the raw Avro value):

```
x-original-topic:orders,x-original-partition:2,x-original-offset:1,x-original-key:1003,
x-error-reason:order validation failed,x-error-class:ValueError,
x-error-detail:price must be non-negative, got -254.75999450683594,
x-failed-at:2026-09-09T03:13:15.795272+00:00,x-retry-count:0    <avro bytes>
```

## 7. Stop and show the summary

`Ctrl-C` terminal A:

```
=== SUMMARY ===
  processed (OK)  : 16
  retries issued  : 14
  sent to DLQ     : 4
  running average : $297.45
  min / max price : $15.69 / $485.08
  per-product avg : Headphones=$358.13, Keyboard=$243.82, Laptop=$192.28, ...
```

## 8. Teardown

```bash
make down             # docker compose down -v
```
