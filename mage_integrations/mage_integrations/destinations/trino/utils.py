from typing import Dict

from mage_integrations.destinations.constants import (
    COLUMN_TYPE_OBJECT,
    COLUMN_TYPE_STRING,
)
from mage_integrations.destinations.sql.utils import (
    convert_column_to_type as convert_column_to_type_orig,
)
from mage_integrations.destinations.sql.utils import convert_column_type as convert_column_type_orig


def convert_column_type(
    column_type: str,
    column_settings: Dict,
    **kwargs,
) -> str:
    if COLUMN_TYPE_OBJECT == column_type:
        return 'JSON'
    if COLUMN_TYPE_STRING == column_type:
        # Strings and date-times were VARCHAR(255) and VARCHAR(52), and the CAST to them
        # cut longer text.
        return 'VARCHAR'

    return convert_column_type_orig(column_type, column_settings, **kwargs)


def convert_column_to_type(value, column_type: str) -> str:
    if 'JSON' == column_type.upper():
        # A CAST of text to JSON made a JSON string of the text.
        return f"JSON '{value}'"

    return convert_column_to_type_orig(value, column_type)
