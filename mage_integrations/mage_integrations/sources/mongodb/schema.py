"""
The schema of MongoDB collections, from the types their documents hold: for each top-level
field, how many values had each BSON type. column_schema in __init__.py turns the counts
into the catalog's JSON schema.

It replaces pymongo-schema, which pulled in GPL-3.0 packages, one of them built from source
with a C compiler, for this one job.
"""
import datetime
import uuid
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional

from bson import Binary, Code, Decimal128, ObjectId, Regex, Timestamp
from bson.int64 import Int64

# Type names column_schema reads: integer and biginteger, float, boolean, OBJECT, ARRAY,
# date and timestamp (text with a datetime format), null; anything else is text.
_TYPE_NAMES = (
    (bool, 'boolean'),  # before int: True is an int
    (Int64, 'biginteger'),
    (int, 'integer'),
    (float, 'float'),
    (Decimal128, 'float'),
    (str, 'string'),
    (ObjectId, 'oid'),
    (datetime.datetime, 'date'),
    (Timestamp, 'timestamp'),
    (dict, 'OBJECT'),
    (list, 'ARRAY'),
    (Binary, 'string'),
    (bytes, 'string'),
    (uuid.UUID, 'string'),
    (Regex, 'string'),
    (Code, 'string'),
)


def type_name(value: Any) -> str:
    if value is None:
        return 'null'
    for python_type, name in _TYPE_NAMES:
        if isinstance(value, python_type):
            return name
    return 'string'


def infer_fields(documents: Iterable[Dict]) -> Dict[str, Dict]:
    """{field: {'types_count': {type name: count}}} over the documents' top-level fields."""
    counts: Dict[str, Counter] = {}
    for document in documents:
        for field, value in document.items():
            counts.setdefault(field, Counter())[type_name(value)] += 1
    return {field: dict(types_count=dict(count)) for field, count in counts.items()}


def collection_documents(collection, sample_size: int = 0):
    """Every document, or a random sample of sample_size when it is above 0."""
    if sample_size and sample_size > 0:
        return collection.aggregate([{'$sample': {'size': int(sample_size)}}])
    return collection.find({})


def extract_schema(
    client,
    database: str,
    collection_names: Optional[List[str]] = None,
    sample_size: int = 0,
) -> Dict[str, Dict[str, Dict]]:
    """{collection: fields} for the named collections, or every collection but system ones."""
    db = client[database]
    names = collection_names or [
        name for name in db.list_collection_names() if not name.startswith('system.')
    ]
    return {
        name: infer_fields(collection_documents(db[name], sample_size))
        for name in names
    }
