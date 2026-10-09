import uuid
from datetime import datetime, timedelta


def timedelta_text(value: timedelta) -> str:
    """
    A duration as [-]H:MM:SS[.ffffff] in total hours, which MySQL's TIME and PostgreSQL's
    interval read. str() wrote '1 day, 0:00:05', which neither reads.
    """
    microseconds = value // timedelta(microseconds=1)
    sign = '-' if microseconds < 0 else ''
    microseconds = abs(microseconds)
    seconds, fraction = divmod(microseconds, 1_000_000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    text = f'{sign}{hours}:{minutes:02d}:{seconds:02d}'
    return f'{text}.{fraction:06d}' if fraction else text


def bytes_text(value) -> str:
    """Bytes in PostgreSQL's bytea text format."""
    return '\\x' + bytes(value).hex()


def encode_complex(obj):
    if hasattr(obj, 'isoformat') and 'method' in type(obj.isoformat).__name__:
        return obj.isoformat()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        # Used to encode the TIME type from the source DB
        return timedelta_text(obj)
    elif type(obj) is uuid.UUID:
        return str(obj)
    elif isinstance(obj, memoryview):
        # PostgreSQL's bytea; it raised "Circular reference detected".
        return bytes_text(obj)
    elif isinstance(obj, (set, frozenset)):
        # MySQL's SET, as its comma-separated text; it raised "Circular reference detected".
        return ','.join(sorted(str(item) for item in obj))

    return obj


def binary_as_text(value):
    """value with bytes that are not UTF-8 in bytea text format, in dicts and lists."""
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode('utf-8')
        except UnicodeDecodeError:
            return bytes_text(value)
    if isinstance(value, dict):
        return {key: binary_as_text(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [binary_as_text(item) for item in value]
    return value
