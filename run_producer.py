#!/usr/bin/env python3
"""Entry point for the order producer.

Examples:
    python run_producer.py --count 50
    python run_producer.py --count 100 --interval 0.1 --poison-rate 0.1 --flaky-rate 0.2
"""

import argparse

from src.config import load_config
from src.producer import OrderProducer


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Produce Avro-encoded order messages.")
    p.add_argument("--count", type=int, default=20, help="number of orders to send")
    p.add_argument("--interval", type=float, default=None,
                   help="seconds to wait between messages (default: config)")
    p.add_argument("--poison-rate", type=float, default=None,
                   help="fraction of messages made invalid, 0..1 (default: config)")
    p.add_argument("--flaky-rate", type=float, default=None,
                   help="fraction of messages tagged to fail transiently (default: config)")
    p.add_argument("--bootstrap", default=None, help="override Kafka bootstrap servers")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config()
    producer = OrderProducer(config, bootstrap_servers=args.bootstrap)
    producer.run(
        count=args.count,
        interval=args.interval,
        poison_rate=args.poison_rate,
        flaky_rate=args.flaky_rate,
    )


if __name__ == "__main__":
    main()
