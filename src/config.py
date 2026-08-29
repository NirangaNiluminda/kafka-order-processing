"""Configuration loader for the Kafka order processing system.

Reads settings from YAML and exposes them as typed dataclasses
for safe, validated access throughout the application.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka broker connection settings."""
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "order-processing-group"


@dataclass(frozen=True)
class TopicConfig:
    """Kafka topic names for each message channel."""
    orders: str = "orders"
    retry: str = "orders-retry"
    dlq: str = "orders-dlq"


@dataclass(frozen=True)
class PriceRange:
    """Min/max bounds for randomly generated prices."""
    min: float = 10.0
    max: float = 500.0


@dataclass(frozen=True)
class ProducerConfig:
    """Settings for the order producer."""
    batch_size: int = 10
    price_range: PriceRange = field(default_factory=PriceRange)
    products: List[str] = field(default_factory=lambda: [
        "Laptop", "Smartphone", "Headphones", "Keyboard", "Monitor"
    ])


@dataclass(frozen=True)
class ConsumerConfig:
    """Settings for the order consumer."""
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    poll_timeout_seconds: float = 1.0


@dataclass(frozen=True)
class RetryConfig:
    """Retry policy with exponential backoff parameters."""
    max_retries: int = 3
    initial_delay_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 30.0


@dataclass(frozen=True)
class SchemaConfig:
    """Path to the Avro schema file."""
    path: str = "schemas/order.avsc"


@dataclass(frozen=True)
class AppConfig:
    """Root configuration container for the entire application."""
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    topics: TopicConfig = field(default_factory=TopicConfig)
    producer: ProducerConfig = field(default_factory=ProducerConfig)
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    schema: SchemaConfig = field(default_factory=SchemaConfig)


def _build_nested(data: dict, cls):
    """Recursively construct a dataclass from a nested dictionary."""
    if data is None:
        return cls()

    field_types = {f.name: f.type for f in cls.__dataclass_fields__.values()}
    kwargs = {}

    for key, value in data.items():
        if key not in field_types:
            continue

        annotation = field_types[key]

        # Handle nested dataclass fields
        if isinstance(value, dict) and hasattr(annotation, "__dataclass_fields__"):
            kwargs[key] = _build_nested(value, annotation)
        else:
            kwargs[key] = value

    return cls(**kwargs)


def load_config(config_path: str = None) -> AppConfig:
    """Load application configuration from a YAML file.

    Resolves the config path relative to the project root directory.
    Falls back to defaults if the file is not found.

    Args:
        config_path: Optional override path to the YAML config file.

    Returns:
        Fully populated AppConfig instance.
    """
    if config_path is None:
        project_root = Path(__file__).resolve().parent.parent
        config_path = project_root / "config" / "settings.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        print(f"[WARN] Config file not found at {config_path}, using defaults.")
        return AppConfig()

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f) or {}

    return AppConfig(
        kafka=_build_nested(raw.get("kafka"), KafkaConfig),
        topics=_build_nested(raw.get("topics"), TopicConfig),
        producer=_build_nested(
            _parse_producer_config(raw.get("producer", {})), ProducerConfig
        ),
        consumer=_build_nested(raw.get("consumer"), ConsumerConfig),
        retry=_build_nested(raw.get("retry"), RetryConfig),
        schema=_build_nested(raw.get("schema"), SchemaConfig),
    )


def _parse_producer_config(data: dict) -> dict:
    """Handle the nested price_range within producer config."""
    if data is None:
        return {}

    result = dict(data)
    if "price_range" in result and isinstance(result["price_range"], dict):
        result["price_range"] = PriceRange(**result["price_range"])

    return result
