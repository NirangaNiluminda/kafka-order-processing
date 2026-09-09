#!/usr/bin/env python3
"""Entry point for the order consumer.

Examples:
    python run_consumer.py
    python run_consumer.py --group demo --transient-rate 0.15
"""

import argparse

from src.config import load_config
from src.consumer import OrderConsumer


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Consume order messages with retry-topic + DLQ handling."
    )
    p.add_argument("--bootstrap", default=None, help="override Kafka bootstrap servers")
    p.add_argument("--group", default=None, help="override consumer group id")
    p.add_argument("--transient-rate", type=float, default=None,
                   help="baseline probability of a simulated transient failure, 0..1")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config()
    consumer = OrderConsumer(
        config,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        transient_rate=args.transient_rate,
    )
    consumer.run()


if __name__ == "__main__":
    main()
