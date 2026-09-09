"""Helpers for working with Kafka message headers.

confluent-kafka represents headers as a ``list[tuple[str, bytes]]`` (or ``None``).
These helpers convert to/from a plain ``dict[str, str]`` and merge overrides while
preserving the original headers.
"""

from typing import Dict, List, Optional, Tuple

RawHeaders = Optional[List[Tuple[str, Optional[bytes]]]]


def to_dict(raw: RawHeaders) -> Dict[str, str]:
    """Decode header values to ``str`` (best effort), keeping the last value per key."""
    out: Dict[str, str] = {}
    for key, value in (raw or []):
        if isinstance(value, (bytes, bytearray)):
            try:
                value = value.decode()
            except UnicodeDecodeError:
                value = value.hex()
        out[key] = value
    return out


def int_header(headers: Dict[str, str], name: str, default: int = 0) -> int:
    try:
        return int(headers[name])
    except (KeyError, TypeError, ValueError):
        return default


def merge(original: RawHeaders, overrides: Dict[str, object]) -> List[Tuple[str, bytes]]:
    """Return ``original`` headers with ``overrides`` applied (values coerced to bytes)."""
    merged: Dict[str, bytes] = {}
    for key, value in (original or []):
        if value is None:
            continue
        merged[key] = bytes(value) if isinstance(value, (bytes, bytearray)) else str(value).encode()
    for key, value in overrides.items():
        merged[key] = value if isinstance(value, (bytes, bytearray)) else str(value).encode()
    return list(merged.items())
