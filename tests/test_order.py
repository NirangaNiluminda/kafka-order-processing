import pytest

from src.models.order import Order


def test_valid_order_round_trips_through_avro_dict():
    order = Order(order_id="1001", product="Laptop", price=999.99)
    as_dict = order.to_avro_dict()
    assert as_dict == {"orderId": "1001", "product": "Laptop", "price": 999.99}
    assert Order.from_avro_dict(as_dict) == order


@pytest.mark.parametrize("kwargs", [
    {"order_id": "", "product": "Laptop", "price": 10.0},
    {"order_id": "1", "product": "", "price": 10.0},
    {"order_id": "1", "product": "Laptop", "price": -0.01},
])
def test_invalid_fields_raise_value_error(kwargs):
    with pytest.raises(ValueError):
        Order(**kwargs)


def test_from_avro_dict_missing_field_raises_value_error():
    with pytest.raises(ValueError):
        Order.from_avro_dict({"orderId": "1", "product": "Laptop"})
