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
    # CREATE [OR REPLACE] [TEMP | TEMPORARY | UNLOGGED | ...] TABLE [IF NOT EXISTS]: CREATE
    # OR REPLACE TABLE (DuckDB, Snowflake) was not recognized, so a raw SQL block passed
    # the statement's status on instead of the table.
    create_table_pattern = (
        r'create(?:\s+or\s+replace)?'
        r'(?:\s+(?:temp|temporary|unlogged|global|local|transient|external))*'
        r'\s+table(?:\s+if\s+not\s+exists)?'
    )

    # The name after the pattern: quoted, or up to a space or a parenthesis. A statement
    # without a parenthesis (CREATE TABLE t AS SELECT * FROM s) was not recognized.
    match = re.search(
        create_table_pattern + r'\s+((?:"(?:[^"]|"")*"|[^\s(;])+)',
        remove_comments(text),
        re.IGNORECASE,
    )
    return match.group(1) if match else None


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


_DOLLAR_TAG = re.compile(r'\$([A-Za-z_][A-Za-z0-9_]*)?\$')


def _statement_ends(text: str) -> List[int]:
    """
    The positions of the semicolons that end statements: not inside a quoted string
    ('...', E'...'), a quoted identifier ("..."), a dollar-quoted string ($$...$$,
    $tag$...$tag$), a line comment (-- ...) or a block comment (/* ... */, nested).
    """
    ends = []
    i, length = 0, len(text)
    while i < length:
        char = text[i]
        pair = text[i:i + 2]
        if pair == '--':
            newline = text.find('\n', i)
            i = length if newline < 0 else newline + 1
        elif pair == '/*':
            depth, i = 1, i + 2
            while i < length and depth:
                if text[i:i + 2] == '/*':
                    depth, i = depth + 1, i + 2
                elif text[i:i + 2] == '*/':
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
        elif char == "'":
            escaped = i > 0 and text[i - 1] in 'eE' and (i < 2 or not text[i - 2].isalnum())
            i += 1
            while i < length:
                if escaped and text[i] == '\\':
                    i += 2
                elif text[i] == "'":
                    if text[i + 1:i + 2] == "'":
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    i += 1
        elif char == '"':
            i += 1
            while i < length:
                if text[i] == '"':
                    if text[i + 1:i + 2] == '"':
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    i += 1
        elif char == '$' and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == '_')):
            match = _DOLLAR_TAG.match(text, i)
            if match:
                closing = text.find(match.group(0), match.end())
                i = length if closing < 0 else closing + len(match.group(0))
            else:
                i += 1
        elif char == ';':
            ends.append(i)
            i += 1
        else:
            i += 1
    return ends


def _only_comments(statement: str) -> bool:
    without = re.sub(r'/\*.*?\*/', '', statement, flags=re.DOTALL)
    without = '\n'.join(line.split('--', 1)[0] for line in without.split('\n'))
    return not without.strip()


def _without_leading_comments(statement: str) -> str:
    while True:
        statement = statement.lstrip()
        if statement.startswith('--'):
            newline = statement.find('\n')
            statement = '' if newline < 0 else statement[newline + 1:]
        elif statement.startswith('/*'):
            end = statement.find('*/')
            statement = '' if end < 0 else statement[end + 2:]
        else:
            return statement


def split_query_string(query_string: str) -> List[str]:
    """
    The statements of a SQL script, split on the semicolons that end them. Semicolons in
    strings, quoted identifiers, dollar-quoted bodies and comments stay in their
    statement. Statements that hold only comments are left out.
    """
    statements, previous = [], 0
    for end in _statement_ends(query_string) + [len(query_string)]:
        statement = _without_leading_comments(query_string[previous:end]).strip()
        previous = end + 1
        if statement and not _only_comments(statement):
            statements.append(statement)
    return statements
