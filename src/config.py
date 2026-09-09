"""Configuration loader for the Kafka order processing system.

Reads settings from YAML and exposes them as typed dataclasses
for safe, validated access throughout the application.
"""

from dataclasses import dataclass, field, fields, is_dataclass
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
    start_order_id: int = 1001
    message_interval_seconds: float = 0.2
    poison_rate: float = 0.0
    flaky_rate: float = 0.0
    price_range: PriceRange = field(default_factory=PriceRange)
    products: List[str] = field(default_factory=lambda: [
        "Laptop", "Smartphone", "Headphones", "Keyboard", "Monitor",
    ])


@dataclass(frozen=True)
class ConsumerConfig:
    """Settings for the order consumer."""
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    poll_timeout_seconds: float = 1.0
    transient_failure_rate: float = 0.0


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


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _build(cls, data):
    """Recursively construct a (frozen) dataclass from a nested dictionary.

    Unknown keys are ignored; missing keys fall back to the dataclass default.
    Nested dataclass fields (e.g. ``ProducerConfig.price_range``) are built
    recursively from their corresponding sub-dictionary.
    """
    if not data:
        return cls()

    field_types = {f.name: f.type for f in fields(cls)}
    kwargs = {}
    for key, value in data.items():
        annotation = field_types.get(key)
        if annotation is None:
            continue
        if is_dataclass(annotation) and isinstance(value, dict):
            kwargs[key] = _build(annotation, value)
        else:
            kwargs[key] = value
    return cls(**kwargs)


def load_config(config_path: str = None) -> AppConfig:
    """Load application configuration from a YAML file.

    Args:
        config_path: Optional override path to the YAML config file. When omitted,
            ``config/settings.yaml`` under the project root is used.

    Returns:
        Fully populated ``AppConfig`` instance. Falls back to defaults when the
        file is absent.
    """
    path = Path(config_path) if config_path else PROJECT_ROOT / "config" / "settings.yaml"

    if not path.exists():
        print(f"[WARN] Config file not found at {path}, using defaults.")
        return AppConfig()

    with open(path, "r") as f:
        raw = yaml.safe_load(f) or {}

    return AppConfig(
        kafka=_build(KafkaConfig, raw.get("kafka")),
        topics=_build(TopicConfig, raw.get("topics")),
        producer=_build(ProducerConfig, raw.get("producer")),
        consumer=_build(ConsumerConfig, raw.get("consumer")),
        retry=_build(RetryConfig, raw.get("retry")),
        schema=_build(SchemaConfig, raw.get("schema")),
    )
