from typing import Dict, List

from mage_integrations.destinations.mysql.constants import RESERVED_WORDS
from mage_integrations.destinations.sql.constants import SQL_RESERVED_WORDS
from mage_integrations.destinations.sql.utils import (
    convert_column_to_type as convert_column_to_type_orig,
)
from mage_integrations.destinations.utils import (
    clean_column_name as clean_column_name_orig,
)
from mage_integrations.sources.constants import (
    COLUMN_FORMAT_DATETIME,
    COLUMN_TYPE_BOOLEAN,
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_NULL,
    COLUMN_TYPE_NUMBER,
    COLUMN_TYPE_OBJECT,
    COLUMN_TYPE_STRING,
)


def clean_column_name(col, lower_case: bool = True):
    col_new = clean_column_name_orig(col, lower_case=lower_case)
    if col_new.upper() in (RESERVED_WORDS + SQL_RESERVED_WORDS):
        col_new = f'_{col_new}'
    return col_new


def column_create_type(column_type_dict: Dict, is_key: bool = False) -> str:
    """
    The MySQL type of a new column. The types were INT, which could not hold a BIGINT,
    CHAR(52) for booleans and date-times, which stored them as text, and CHAR(255) for
    strings, which held 255 characters and dropped trailing spaces.
    """
    column_type = column_type_dict['type']
    if COLUMN_TYPE_BOOLEAN == column_type:
        return 'BOOLEAN'
    if COLUMN_TYPE_INTEGER == column_type:
        minimum = column_type_dict['column_settings'].get('minimum')
        # A source's unsigned columns hold values above 2**63.
        return 'BIGINT UNSIGNED' if minimum is not None and minimum >= 0 else 'BIGINT'
    if COLUMN_TYPE_NUMBER == column_type:
        return 'DOUBLE'
    if COLUMN_TYPE_OBJECT == column_type:
        return 'JSON'
    if COLUMN_FORMAT_DATETIME == column_type_dict['column_settings'].get('format'):
        return 'DATETIME(6)'
    if column_type_dict['type_converted'] == 'LONGTEXT':
        # Arrays, as JSON text.
        return 'LONGTEXT'
    # Keys and unique columns need a length.
    return 'VARCHAR(255)' if is_key else 'LONGTEXT'


def build_alter_table_command(
    column_type_mapping: Dict,
    columns: List[str],
    full_table_name: str,
    column_identifier: str = '',
    use_lowercase: bool = True,
) -> str:
    if not columns:
        return None

    columns_and_types = []
    for col in columns:
        column_name = \
            f'{column_identifier}{clean_column_name(col, lower_case=use_lowercase)}'\
            f'{column_identifier}'
        column_type = column_create_type(column_type_mapping[col])
        col_statement = f'ADD COLUMN {column_name} {column_type}'
        columns_and_types.append(col_statement)

    # TODO: support add new unique constraints
    return f"ALTER TABLE {full_table_name} {', '.join(columns_and_types)}"


def build_create_table_command(
    column_type_mapping: Dict,
    columns: List[str],
    full_table_name: str,
    key_properties: List[str],
    schema: Dict,
    unique_constraints: List[str] = None,
    use_lowercase: bool = True,
) -> str:
    columns_and_types = []
    column_properties = schema['properties']

    keys = set(unique_constraints or []) | set(key_properties or [])
    for col in columns:
        column_name = clean_column_name(col, lower_case=use_lowercase)
        column_type = column_create_type(column_type_mapping[col], is_key=col in keys)

        column_props = []
        if COLUMN_TYPE_NULL not in column_properties.get(col).get('type', []):
            column_props.append('NOT NULL')

        col_statement = f'{column_name} {column_type}'
        if len(column_props) >= 1:
            col_statement = f"{col_statement} {' '.join(column_props)}"
        columns_and_types.append(col_statement)

    if unique_constraints:
        unique_constraints = [clean_column_name(col, lower_case=use_lowercase)
                              for col in unique_constraints]
        index_name = '_'.join([
            clean_column_name(full_table_name, lower_case=use_lowercase),
        ] + unique_constraints)
        index_name = f'unique{index_name}'[:64]
        columns_and_types.append(f"CONSTRAINT {index_name} Unique({', '.join(unique_constraints)})")

    if key_properties and len(key_properties) >= 1:
        col = clean_column_name(key_properties[0], lower_case=use_lowercase)
        columns_and_types.append(f'PRIMARY KEY ({col})')

    return f"CREATE TABLE {full_table_name} ({', '.join(columns_and_types)})"


def convert_column_to_type(value, column_type: str):
    if 'DOUBLE' in column_type:
        return f'{value}'
    if column_type == 'SIGNED':
        # Integers are written as numbers. CAST to SIGNED wrapped values above 2**63 of
        # unsigned columns around to negative ones; booleans are written as 1 and 0.
        if value in ('True', 'False'):
            return '1' if value == 'True' else '0'
        try:
            return str(int(value))
        except ValueError:
            pass

    return convert_column_to_type_orig(value, column_type)


def convert_column_type(column_type: str, column_settings: Dict, **kwargs) -> str:
    """
    The type values are cast to in INSERT statements, which MySQL's CAST takes. Integers
    were cast to UNSIGNED, which wrapped every negative value around, so it was out of
    range; strings to CHAR(255), which cut them at 255 characters.
    """
    if COLUMN_TYPE_BOOLEAN == column_type:
        return 'SIGNED'
    elif COLUMN_TYPE_INTEGER == column_type:
        return 'SIGNED'
    elif COLUMN_TYPE_NUMBER == column_type:
        return 'DOUBLE'
    elif COLUMN_TYPE_OBJECT == column_type:
        return 'JSON'
    elif COLUMN_TYPE_STRING == column_type:
        if COLUMN_FORMAT_DATETIME == column_settings.get('format'):
            return 'DATETIME(6)'

    return 'CHAR'
