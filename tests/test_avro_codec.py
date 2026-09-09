import pytest

from src.config import PROJECT_ROOT
from src.serialization import AvroCodec, AvroDecodeError

SCHEMA = PROJECT_ROOT / "schemas" / "order.avsc"


@pytest.fixture
def codec():
    return AvroCodec(SCHEMA)


def test_encode_decode_round_trip(codec):
    record = {"orderId": "1001", "product": "Laptop", "price": 999.99}
    decoded = codec.decode(codec.encode(record))
    assert decoded["orderId"] == "1001"
    assert decoded["product"] == "Laptop"
    # price is an Avro float (32-bit) -> tolerate precision loss
    assert decoded["price"] == pytest.approx(999.99, rel=1e-5)


def test_decode_empty_payload_raises(codec):
    with pytest.raises(AvroDecodeError):
        codec.decode(b"")


def test_decode_garbage_raises(codec):
    with pytest.raises(AvroDecodeError):
        codec.decode(b"\xff\xff\xff\xff\xff\xff")


def test_encode_missing_field_raises(codec):
    with pytest.raises(ValueError):
        codec.encode({"orderId": "1", "product": "Laptop"})


def test_missing_schema_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        AvroCodec(tmp_path / "nope.avsc")
