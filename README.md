<div align="center">

# 📦 Kafka Order Processing

**A fault-tolerant Kafka pipeline in Python — Avro-encoded orders, real-time aggregation,
retry topic with exponential backoff, and a Dead Letter Queue.**

![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-confluent--kafka-231F20?logo=apachekafka&logoColor=white)
![Avro](https://img.shields.io/badge/Avro-fastavro-1E88E5)
![Docker](https://img.shields.io/badge/Docker%20Compose-2496ED?logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/tests-29-brightgreen)

</div>

<!-- TODO: add a short GIF of the live demo (producer + consumer + kafka-ui showing a message landing in the DLQ) -->

## What it shows

Happy-path Kafka is easy. This project is about what happens when processing **fails**:

```mermaid
flowchart LR
    P[OrderProducer] -->|Avro · key=orderId| ORD[(orders)]
    ORD --> C{{OrderConsumer}}
    RET[(orders-retry)] --> C
    C -->|success| AGG[PriceAggregator<br/>running average]
    C -->|transient failure<br/>attempt &lt; max| RET
    C -->|permanent / retries exhausted| DLQ[(orders-dlq)]
```

- **Avro serialization** with a shared `.avsc` schema; `orderId` as key for partition affinity.
- **Retry topic + exponential backoff** — failures leave the main partition, keeping healthy
  traffic flowing.
- **Dead Letter Queue** for poison messages, with failure metadata preserved.
- **At-least-once delivery** — retry/DLQ writes are flushed *before* the source offset is committed.
- **Configurable failure simulation** to demo every path live.

## Design trade-offs (the interview-worthy part)

| Decision | Why | What I'd do at scale |
|---|---|---|
| Retry *topic*, not in-process retry | Keeps ordering for healthy traffic; failures don't block the partition | Tiered delay topics (`retry-1s`, `-5s`, …) for fully non-blocking backoff |
| At-least-once, not exactly-once | Simpler and safe if DLQ consumers are idempotent | Kafka transactions / idempotent producer |
| In-memory aggregation | One consumer owns the running average | Kafka Streams state store or Redis for multi-consumer scaling |
| No Schema Registry | Producer and consumer load the same `.avsc` | Confluent Schema Registry for schema evolution |

## Quick start

```bash
docker compose up -d                 # Kafka + kafka-ui (:8080)
pip install -r requirements.txt
python run_consumer.py               # terminal 1
python run_producer.py               # terminal 2
pytest                               # 29 tests
```

Live demo script: [`docs/DEMO.md`](docs/DEMO.md) · Full design notes: [`SETUP.md`](SETUP.md)

---

**G.W. Niranga Niluminda** · [LinkedIn](https://linkedin.com/in/niranganiluminda)
