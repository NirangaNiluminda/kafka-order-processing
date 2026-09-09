from src.config import AppConfig, load_config


def test_load_config_reads_project_settings():
    config = load_config()
    assert config.topics.orders == "orders"
    assert config.topics.retry == "orders-retry"
    assert config.topics.dlq == "orders-dlq"
    # nested dataclass is built from YAML
    assert config.producer.price_range.max == 500.0
    assert "Laptop" in config.producer.products
    assert config.retry.max_retries == 3
    # new fields have sane defaults / values
    assert config.producer.start_order_id == 1001
    assert 0.0 <= config.consumer.transient_failure_rate <= 1.0


def test_missing_file_falls_back_to_defaults(tmp_path):
    config = load_config(tmp_path / "absent.yaml")
    assert config == AppConfig()
