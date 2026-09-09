"""Order domain model.

Provides a clean abstraction over the raw Avro record,
encapsulating validation and conversion logic.
"""

from dataclasses import dataclass


@dataclass
class Order:
    """Represents a purchase transaction.

    Attributes:
        order_id: Unique identifier for the order.
        product: Name of the purchased item.
        price: Price of the product.
    """
    order_id: str
    product: str
    price: float

    def __post_init__(self):
        """Validate order fields after initialization."""
        if not self.order_id:
            raise ValueError("order_id cannot be empty")
        if not self.product:
            raise ValueError("product cannot be empty")
        if self.price < 0:
            raise ValueError(f"price must be non-negative, got {self.price}")

    def to_avro_dict(self) -> dict:
        """Convert to Avro-compatible dictionary.

        Maps Python snake_case fields to the camelCase Avro schema fields.

        Returns:
            Dictionary matching the order.avsc schema.
        """
        return {
            "orderId": self.order_id,
            "product": self.product,
            "price": self.price,
        }

    @classmethod
    def from_avro_dict(cls, data: dict) -> "Order":
        """Construct an Order from an Avro-deserialized dictionary.

        Args:
            data: Dictionary with camelCase keys from Avro deserialization.

        Returns:
            Order instance.

        Raises:
            ValueError: If a required field is missing or a field fails validation.
                Callers treat this as a permanent (non-retryable) failure.
        """
        missing = [k for k in ("orderId", "product", "price") if k not in data]
        if missing:
            raise ValueError(f"order record missing required field(s): {missing}")
        return cls(
            order_id=data["orderId"],
            product=data["product"],
            price=data["price"],
        )

    def __str__(self) -> str:
        return f"Order(id={self.order_id}, product={self.product}, price=${self.price:.2f})"
