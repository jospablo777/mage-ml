"""
SQL statement helpers: split a query string into statements and find the table that a
CREATE, INSERT, UPDATE or DROP statement names. They use only the standard library, so
exported pipeline services load this module on its own.
"""
import re
from typing import List, Optional, Tuple

MAGE_SEMI_COLON = '__MAGE_SEMI_COLON__'


def extract_and_replace_text_between_strings(
    text: str,
    start_string: str,
    end_string: str = None,
    replace_string: str = '',
    case_sensitive: bool = True,
) -> Tuple[str, str]:
    start_match = re.search(start_string, text, re.NOFLAG if not case_sensitive else re.IGNORECASE)
    if end_string:
        end_match = re.search(end_string, text, re.NOFLAG if not case_sensitive else re.IGNORECASE)
    else:
        end_match = None

    if not start_match or (end_string and not end_match):
        return None, text

    start_idx = start_match.span()[0]
    if end_string and end_match:
        end_idx = end_match.span()[1]

    extracted_text = text[start_idx:end_idx]

    new_text = text[0:max(start_idx - 1, 0)] + replace_string + text[end_idx + 1:]

    return extracted_text, new_text


def remove_comments(text: str) -> str:
    lines = text.split('\n')
    return '\n'.join(line for line in lines if not line.startswith('--'))


def extract_create_statement_table_name(text: str) -> Optional[str]:
    create_table_pattern = r'create table(?: if not exists)*'

    statement_partial, _ = extract_and_replace_text_between_strings(
        remove_comments(text),
        create_table_pattern,
        r'\(',
    )
    if not statement_partial:
        return None

    match1 = re.match(create_table_pattern, statement_partial, re.IGNORECASE)
    if match1:
        idx_start, idx_end = match1.span()
        new_statement = statement_partial[0:idx_start] + statement_partial[idx_end:]
        match2 = re.search(r'[^\s]+', new_statement.strip())
        if match2:
            return match2.group(0)

    parts = statement_partial[:len(statement_partial) - 1].strip().split(' ')
    return parts[-1]


def extract_insert_statement_table_names(text: str) -> List[str]:
    matches = re.findall(
        r'insert(?:\s+ignore)?(?:\s+overwrite)?(?:\s+into)?[\s]+([\w.]+)',
        remove_comments(text),
        re.IGNORECASE,
    )
    return matches


def extract_drop_statement_table_names(text: str) -> List[str]:
    matches = re.findall(
        r'\bdrop\s+table(?:\s+if\s+exists)?\s+([\w.]+)',
        remove_comments(text),
        re.IGNORECASE,
    )
    return matches


def extract_update_statement_table_names(text: str) -> List[str]:
    matches = re.findall(
        r'\bupdate\b\s+([\w.]+)\s+(?:as\s+\w+\s+)?set\s+[\s\S]*?\bwhere\b',
        remove_comments(text),
        re.IGNORECASE,
    )
    return matches


def extract_full_table_name(text: str) -> Optional[str]:
    if not text:
        return None

    table_name = extract_create_statement_table_name(text)
    if table_name:
        return table_name

    matches = extract_insert_statement_table_names(text)
    if len(matches) == 0:
        matches = extract_update_statement_table_names(text)

    if len(matches) == 0:
        return None

    return matches[len(matches) - 1]


def has_create_or_insert_statement(text: str) -> bool:
    table_name = extract_create_statement_table_name(text)
    if table_name:
        return True

    matches = extract_insert_statement_table_names(text)
    return len(matches) >= 1


def has_drop_statement(text: str) -> bool:
    matches = extract_drop_statement_table_names(text)
    return len(matches) >= 1


def has_update_statement(text: str) -> bool:
    matches = extract_update_statement_table_names(text)
    return len(matches) >= 1


def split_query_string(query_string: str) -> List[str]:
    text_parts = []

    matches = re.finditer(r"'(.*?)'|\"(.*?)\"", query_string, re.IGNORECASE)

    previous_idx = 0

    for _, match in enumerate(matches):
        matched_string = match.group()
        updated_string = re.sub(r';', MAGE_SEMI_COLON, matched_string)

        start_idx, end_idx = match.span()

        previous_chunk = query_string[previous_idx:start_idx]
        text_parts.append(previous_chunk)
        text_parts.append(updated_string)
        previous_idx = end_idx

    text_parts.append(query_string[previous_idx:])

    text_combined = ''.join(text_parts)
    queries = text_combined.split(';')

    arr = []
    for query in queries:
        query = query.strip()
        if not query:
            continue

        lines = query.split('\n')
        query = '\n'.join(list(filter(lambda x: not x.startswith('--'), lines)))
        query = query.strip()
        query = re.sub(MAGE_SEMI_COLON, ';', query)

        if query:
            arr.append(query)

    return arr
