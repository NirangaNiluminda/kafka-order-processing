"""Avro serialization for order messages.

Uses fastavro's *schemaless* binary encoding: the schema is not embedded in every
message (there is no Schema Registry in this project). Producer and consumer both
load the same ``schemas/order.avsc`` file, so the reader schema always matches the
writer schema.
"""

import io
import json
from pathlib import Path
from typing import Union

import fastavro


class AvroDecodeError(Exception):
    """Raised when a message payload cannot be decoded against the order schema.

    Treated by the consumer as a permanent (non-retryable) failure.
    """


class AvroCodec:
    """Encodes/decodes order dictionaries to and from Avro binary."""

    def __init__(self, schema_path: Union[str, Path]):
        path = Path(schema_path)
        if not path.exists():
            raise FileNotFoundError(f"Avro schema not found at {path}")
        with open(path, "r") as f:
            raw_schema = json.load(f)
        # parse_schema validates the schema and expands named types.
        self._schema = fastavro.parse_schema(raw_schema)

    @property
    def schema(self) -> dict:
        return self._schema

    def encode(self, record: dict) -> bytes:
        """Serialize an Avro-compatible dict (camelCase keys) to bytes.

        Raises:
            ValueError: If the record does not conform to the schema.
        """
        buffer = io.BytesIO()
        try:
            fastavro.schemaless_writer(buffer, self._schema, record)
        except Exception as exc:  # fastavro raises ValueError / TypeError on bad data
            raise ValueError(f"record does not conform to order schema: {exc}") from exc
        return buffer.getvalue()

    def decode(self, payload: bytes) -> dict:
        """Deserialize Avro bytes back into a dict with camelCase keys.

        Raises:
            AvroDecodeError: If the payload is missing or cannot be parsed.
        """
        if not payload:
            raise AvroDecodeError("empty payload")
        try:
            return fastavro.schemaless_reader(io.BytesIO(payload), self._schema)
        except Exception as exc:
            raise AvroDecodeError(f"could not decode order payload: {exc}") from exc
