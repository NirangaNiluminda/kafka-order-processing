"""Real-time price aggregation.

Maintains a running average (and a few companion statistics) over every order
price the consumer successfully processes. State is in-memory and owned by a
single consumer instance -- see the README for notes on scaling this to multiple
consumers with a shared store or Kafka Streams.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class _ProductStats:
    count: int = 0
    total: float = 0.0

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0


@dataclass
class PriceAggregator:
    """Accumulates order prices and exposes running statistics."""

    count: int = 0
    total: float = 0.0
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    _by_product: Dict[str, _ProductStats] = field(default_factory=dict)

    def add(self, price: float, product: Optional[str] = None) -> float:
        """Record one price and return the updated running average."""
        self.count += 1
        self.total += price
        self.min_price = price if self.min_price is None else min(self.min_price, price)
        self.max_price = price if self.max_price is None else max(self.max_price, price)

        if product is not None:
            stats = self._by_product.setdefault(product, _ProductStats())
            stats.count += 1
            stats.total += price

        return self.running_average

    @property
    def running_average(self) -> float:
        return self.total / self.count if self.count else 0.0

    def per_product_average(self) -> Dict[str, float]:
        return {name: s.average for name, s in sorted(self._by_product.items())}

    def snapshot(self) -> dict:
        """Return a plain-dict summary, suitable for logging."""
        return {
            "count": self.count,
            "running_average": round(self.running_average, 2),
            "min_price": self.min_price,
            "max_price": self.max_price,
            "per_product_average": {
                k: round(v, 2) for k, v in self.per_product_average().items()
            },
        }
