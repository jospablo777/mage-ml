import json
from typing import Dict, List

from mage_integrations.destinations.constants import (
    COLUMN_FORMAT_DATETIME,
    COLUMN_TYPE_OBJECT,
    COLUMN_TYPE_STRING,
)
from mage_integrations.destinations.sql.utils import (
    convert_column_type as convert_column_type_orig,
)


def escape_quotes(line: str, single: bool = True, double: bool = True) -> str:
    new_line = str(line)
    if single:
        new_line = new_line.replace("'", "''")
    if double:
        new_line = new_line.replace('\"', '\\"')
    return new_line


def array_literal(values: List) -> str:
    """
    A PostgreSQL array literal: elements are quoted and escaped, None is NULL, and nested
    lists are nested arrays. Elements were joined unquoted, so None became the text None,
    text with commas or braces split, and the lists of a 2-D array were written as
    [1, 2].
    """
    items = []
    for value in values:
        if value is None:
            items.append('NULL')
        elif isinstance(value, (list, tuple)):
            items.append(array_literal(value))
        elif isinstance(value, bool):
            items.append('true' if value else 'false')
        elif isinstance(value, (int, float)):
            items.append(str(value))
        else:
            text = json.dumps(value) if isinstance(value, dict) else str(value)
            items.append('"' + text.replace('\\', '\\\\').replace('"', '\\"') + '"')
    return '{' + ','.join(items) + '}'


def convert_array(v: List, column_type_dict: Dict):
    item_type_converted = column_type_dict['item_type_converted']

    if 'JSONB' == item_type_converted.upper():
        arr = []
        for v2 in v:
            if v2 is None:
                # None was written as 'None', which is not JSON.
                arr.append('NULL')
                continue
            if not isinstance(v2, str):
                v2 = json.dumps(v2)
            arr.append(f"'{escape_quotes(v2, double=False)}'")
        value_final = f"ARRAY[{', '.join(arr)}]::JSONB[]"
    else:
        value_final = "'" + array_literal(v).replace("'", "''") + "'"

    return value_final


def convert_column_type(
    column_type: str,
    column_settings: Dict,
    **kwargs,
) -> str:
    if COLUMN_TYPE_OBJECT == column_type:
        return 'JSONB'
    elif COLUMN_TYPE_STRING == column_type \
            and COLUMN_FORMAT_DATETIME == column_settings.get('format'):
        return 'TIMESTAMPTZ'

    return convert_column_type_orig(column_type, column_settings, **kwargs)
