# Kafka Order Processing System

A real-time order processing pipeline built with Apache Kafka and Avro serialization. This system demonstrates producer-consumer messaging patterns with fault-tolerant processing including retry logic and Dead Letter Queue (DLQ) handling.

## Features

- **Avro Serialization** — Type-safe message encoding with schema definition
- **Real-time Aggregation** — Running average price calculation across all processed orders
- **Retry Logic** — Exponential backoff for transient processing failures
- **Dead Letter Queue** — Poison message isolation for permanently failed orders
- **Dockerized Infrastructure** — Single-command Kafka cluster setup

## Prerequisites

- Docker & Docker Compose
- Python 3.9+
- pip

## Quick Start

### 1. Start Kafka Infrastructure

```bash
docker-compose up -d
```

Wait for the `kafka-init` container to finish creating topics:

```bash
docker-compose logs -f kafka-init
```

### 2. Install Python Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the Producer

```bash
python run_producer.py
```

The producer generates randomized order messages and publishes them to the `orders` topic.

### 4. Run the Consumer

In a separate terminal:

```bash
source venv/bin/activate
python run_consumer.py
```

The consumer processes orders, calculates running averages, and handles failures with retry + DLQ.

## Project Structure

```
kafka-order-processing/
├── docker-compose.yml          # Kafka + Zookeeper infrastructure
├── requirements.txt            # Python dependencies
├── schemas/
│   └── order.avsc              # Avro schema for order messages
├── config/
│   └── settings.yaml           # Application configuration
├── src/
│   ├── config.py               # Configuration loader
│   ├── models/
│   │   └── order.py            # Order domain model
│   ├── serialization/
│   │   └── avro_serializer.py  # Avro encode/decode
│   ├── producer/
│   │   └── order_producer.py   # Kafka message producer
│   ├── consumer/
│   │   ├── order_consumer.py   # Kafka message consumer
│   │   ├── retry_handler.py    # Retry with exponential backoff
│   │   └── dlq_handler.py      # Dead Letter Queue handler
│   └── aggregation/
│       └── price_aggregator.py # Running average calculator
├── run_producer.py             # Producer entry point
└── run_consumer.py             # Consumer entry point
```

## Configuration

All settings are centralized in `config/settings.yaml`. Key options:

| Setting | Description |
|---------|-------------|
| `kafka.bootstrap_servers` | Kafka broker address |
| `producer.batch_size` | Number of orders per batch |
| `retry.max_retries` | Max retry attempts before DLQ |
| `retry.backoff_multiplier` | Exponential backoff factor |

## Topics

| Topic | Purpose |
|-------|---------|
| `orders` | Primary order message channel |
| `orders-retry` | Retry queue for failed messages |
| `orders-dlq` | Dead Letter Queue for permanently failed messages |

## Teardown

```bash
docker-compose down -v
```
