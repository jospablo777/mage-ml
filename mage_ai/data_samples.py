"""
Masked samples of production tables for local development (`mage data sample <name>`).

A sample is defined in data_samples/<name>.yaml in the project:

    source:
      profile: default          # the io_config.yaml profile it reads with
      kind: postgres            # postgres (the default), mysql or duckdb
    key_env: MAGE_SAMPLE_KEY    # the variable holding the masking key, at least 16 characters
    tables:
      - table: public.customers
        key: id                 # the column the sample is drawn by
        sample: {fraction: 0.05}          # or {rows: 1000}, or {stratify: country, rows: 100}
        where: "created_at >= now() - interval '90 days'"
        columns:
          id: hash              # keyed hash; integer keys stay integers
          email: {fake: email}  # name, email, phone or text, the same for the same value
          name: {fake: name}
          ssn: null             # missing
          note: mask            # *** for text
          birth_date: {shift_days: 30, by: id}
        keep: {country: "a country is not personal here"}
      - table: public.orders
        follow: {table: public.customers, column: customer_id}   # the sampled customers' orders
        columns: {id: hash, customer_id: hash}

Rows are drawn in the source's SQL, by a hash of the key, so the sample of one table and
another sampled by the same key hold the same entities; `follow` takes the rows whose column
holds a key the other table's sample has. Masking runs in Rust (ColumnAtlas `mask_table`)
with the key: the same value hashes or fakes to the same output in every table, so joins on
hashed keys still match, and without the key a hash cannot be checked against a guess.

A column whose name looks personal (email, phone, name, address, birth date, national id,
card, password, token...) and has no rule stops the copy; `keep` takes it as it is, with the
reason. The copy goes to .mage/dev_data/<name>/: one Parquet file per table, a DuckDB file
with the tables, and record.json with what was copied, when, by whom and with which rules.
"""
import getpass
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import yaml

SAMPLES_DIR = 'data_samples'
OUTPUT_DIR = Path('.mage') / 'dev_data'
DEFAULT_KEY_ENV = 'MAGE_SAMPLE_KEY'
MIN_KEY_LENGTH = 16
RULES = ('keep', 'hash', 'null', 'mask')
FAKES = ('name', 'email', 'phone', 'text')
_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$')
# Column names that usually hold people's data: a column like these needs a rule or a
# reason to keep it.
PERSONAL = re.compile(
    r'(^|_)(e_?mail|phone|mobile|cell|ssn|social_security|national_id|tax_id|passport|'
    r'(first|last|full|middle|given|family|customer|user|person|contact|client)_?name|'
    r'surname|address|street|postal|zip(_?code)?|'
    r'birth|dob|iban|account_number|card(_?number)?|cvv|password|secret|token|ip(_address)?|'
    r'license|salary)($|_)',
    re.IGNORECASE,
)
# A column named only `name` usually holds a person's or a company's: it needs a rule too.
BARE_NAME = re.compile(r'^name$', re.IGNORECASE)


class SampleError(Exception):
    def __init__(self, problems: List[str]):
        super().__init__('\n'.join(problems))
        self.problems = problems


def definition_path(repo_path: str, name: str) -> Path:
    return Path(repo_path) / SAMPLES_DIR / f'{name}.yaml'


def load(repo_path: str, name: str) -> Dict:
    path = definition_path(repo_path, name)
    if not path.is_file():
        raise SampleError([f'There is no sample {name}: {path} does not exist.'])
    value = yaml.safe_load(path.read_text()) or {}
    problems = check(value)
    if problems:
        raise SampleError([f'{path.name}: {p}' for p in problems])
    return value


def _rule(column: str, rule) -> Dict:
    """A column's rule in the form mask_table reads."""
    if rule is None:
        return dict(rule='null')
    if isinstance(rule, str):
        if rule not in RULES:
            raise ValueError(f'column {column}: {rule!r} is not one of {", ".join(RULES)}, '
                             'fake or shift_days.')
        return dict(rule=rule)
    if isinstance(rule, dict) and set(rule) == {'fake'}:
        if rule['fake'] not in FAKES:
            raise ValueError(f'column {column}: fake is one of {", ".join(FAKES)}.')
        return dict(rule='fake', kind=rule['fake'])
    if isinstance(rule, dict) and 'shift_days' in rule and set(rule) <= {'shift_days', 'by'}:
        days = rule['shift_days']
        if isinstance(days, bool) or not isinstance(days, int) or days < 1:
            raise ValueError(f'column {column}: shift_days is a number of days above 0.')
        return dict(rule='shift_days', days=days, by=rule.get('by'))
    raise ValueError(f'column {column}: {rule!r} is not a rule.')


def check(value: Dict) -> List[str]:
    """What is wrong with a sample's definition."""
    problems = []
    if not isinstance(value, dict):
        return ['The definition must be a mapping with source and tables.']
    source = value.get('source') or {}
    if not isinstance(source, dict) or not source.get('profile'):
        problems.append('source needs the io_config.yaml profile it reads with.')
    elif source.get('kind', 'postgres') not in SOURCE_KINDS:
        problems.append(f"source kind is one of {', '.join(SOURCE_KINDS)}.")
    tables = value.get('tables') or []
    if not isinstance(tables, list) or not tables:
        problems.append('tables lists at least one table.')
        return problems
    names = [t.get('table') for t in tables if isinstance(t, dict)]
    for table in tables:
        if not isinstance(table, dict) or not _IDENTIFIER.match(str(table.get('table') or '')):
            problems.append(f'{table!r}: each table has a table name, such as public.orders.')
            continue
        where = table['table']
        follow = table.get('follow')
        if follow is not None:
            if (not isinstance(follow, dict) or follow.get('table') not in names
                    or not _IDENTIFIER.match(str(follow.get('column') or ''))):
                problems.append(f'{where}: follow names a table sampled before it and a column.')
            elif names.index(follow['table']) > names.index(where):
                problems.append(f'{where}: follow names a table sampled after it.')
        elif not _IDENTIFIER.match(str(table.get('key') or '')):
            problems.append(f'{where}: key names the column rows are drawn by.')
        sample = table.get('sample') or {}
        if follow is None:
            fraction, rows = sample.get('fraction'), sample.get('rows')
            if (fraction is None) == (rows is None):
                problems.append(f'{where}: sample has a fraction or a number of rows.')
            if fraction is not None and not (isinstance(fraction, (int, float))
                                             and 0 < fraction <= 1):
                problems.append(f'{where}: sample fraction is above 0 and at most 1.')
            if rows is not None and not (isinstance(rows, int) and rows > 0):
                problems.append(f'{where}: sample rows is a number above 0.')
            if sample.get('stratify') and not _IDENTIFIER.match(str(sample['stratify'])):
                problems.append(f'{where}: stratify names a column.')
            if sample.get('stratify') and rows is None:
                problems.append(f'{where}: a stratified sample takes rows per group.')
        for column, rule in (table.get('columns') or {}).items():
            try:
                _rule(column, rule)
            except ValueError as error:
                problems.append(f'{where}: {error}')
        keep = table.get('keep') or {}
        if not isinstance(keep, dict) or not all(isinstance(r, str) and r.strip()
                                                  for r in keep.values()):
            problems.append(f'{where}: keep maps each column kept as it is to the reason.')
    return problems


# Per source: a stable hash of a key as a non-negative integer, and the placeholder of a
# query parameter. The same key gives the same bucket in every table of one source, so the
# samples of tables drawn by the same key hold the same entities.
DIALECTS = {
    'postgres': dict(bucket='abs(hashtextextended(CAST({column} AS text), 0))', parameter='%s'),
    'mysql': dict(
        bucket='CAST(CONV(SUBSTRING(SHA2(CAST({column} AS CHAR), 256), 1, 15), 16, 10) '
               'AS UNSIGNED)',
        parameter='%s',
    ),
    'duckdb': dict(bucket='abs(hash(CAST({column} AS VARCHAR)))', parameter='?'),
}
SOURCE_KINDS = tuple(DIALECTS)


def _hash_bucket(column: str, dialect: str = 'postgres') -> str:
    return DIALECTS[dialect]['bucket'].format(column=column)


def query(table: Dict, followed: Optional[List] = None, dialect: str = 'postgres') -> tuple:
    """The SQL that draws the table's sample, and its parameters."""
    where = table.get('where')
    conditions = [f'({where})'] if where else []
    params = []
    follow = table.get('follow')
    if follow:
        keys = list(followed or [])
        if dialect == 'postgres':
            conditions.append(f"{follow['column']} = ANY(%s)")
            params.append(keys)
        elif keys:
            marks = ', '.join([DIALECTS[dialect]['parameter']] * len(keys))
            conditions.append(f"{follow['column']} IN ({marks})")
            params.extend(keys)
        else:
            # No key was sampled: no row follows.
            conditions.append('1 = 0')
        return (f"SELECT * FROM {table['table']}"
                + (' WHERE ' + ' AND '.join(conditions) if conditions else ''), params)
    sample = table['sample']
    key = table['key']
    if sample.get('fraction') is not None:
        conditions.append(
            f'{_hash_bucket(key, dialect)} % 1000000 < {int(sample["fraction"] * 1000000)}',
        )
        return (f"SELECT * FROM {table['table']} WHERE " + ' AND '.join(conditions), params)
    filtered = f"SELECT * FROM {table['table']}" + (
        ' WHERE ' + ' AND '.join(conditions) if conditions else '')
    if sample.get('stratify'):
        return (
            f"SELECT * FROM (SELECT t.*, row_number() OVER (PARTITION BY {sample['stratify']} "
            f'ORDER BY {_hash_bucket(key, dialect)}) AS mage_sample_rank FROM ({filtered}) t) s '
            f"WHERE mage_sample_rank <= {int(sample['rows'])}", params)
    return f"{filtered} ORDER BY {_hash_bucket(key, dialect)} LIMIT {int(sample['rows'])}", params


def personal_without_rule(table: Dict, columns: List[str]) -> List[str]:
    ruled = set(table.get('columns') or {}) | set(table.get('keep') or {})
    return [c for c in columns
            if (PERSONAL.search(c) or BARE_NAME.match(c)) and c not in ruled]


def run(repo_path: str, name: str, reader=None, out_dir: Optional[str] = None) -> Dict:
    """
    Copies the sample: draws each table's rows, masks them, writes Parquet, a DuckDB file
    and record.json. reader(sql, params) returns a pandas DataFrame; by default it reads
    with the source profile.
    """
    import column_atlas_native as native

    value = load(repo_path, name)
    key_env = value.get('key_env') or DEFAULT_KEY_ENV
    key = os.environ.get(key_env) or ''
    if len(key) < MIN_KEY_LENGTH:
        raise SampleError([
            f'{key_env} must hold the masking key, at least {MIN_KEY_LENGTH} characters: the '
            'same key gives the same hashes, and without it they cannot be checked. Keep it '
            'out of the project, as a secret.'])
    kind = value['source'].get('kind', 'postgres')
    reader = reader or _reader(repo_path, value['source']['profile'], kind)
    out = Path(out_dir) if out_dir else Path(repo_path) / OUTPUT_DIR / name
    # Masked or not, a copy of production data stays out of Git.
    from mage_ai.pipeline_services import definitions

    definitions.ensure_gitignore(repo_path)
    staging = Path(tempfile.mkdtemp(prefix='mage-sample-'))
    sampled_keys: Dict[str, List] = {}
    record_tables = []
    try:
        for table in value['tables']:
            follow = table.get('follow')
            sql, params = query(
                table, sampled_keys.get(follow['table']) if follow else None, kind,
            )
            frame = reader(sql, params)
            frame = frame.drop(columns=['mage_sample_rank'], errors='ignore')
            missing = personal_without_rule(table, list(frame.columns))
            if missing:
                raise SampleError([
                    f"{table['table']}: columns {', '.join(missing)} look personal and have no "
                    'rule: give each a rule (hash, fake, null, mask, shift_days), or keep it '
                    'with the reason.'])
            # Before masking: the tables that follow this one select by the source's values.
            if table.get('key'):
                sampled_keys[table['table']] = frame[table['key']].dropna().tolist()
            raw = staging / f"{table['table']}.parquet"
            frame.to_parquet(raw, index=False)
            rules = {c: _rule(c, r) for c, r in (table.get('columns') or {}).items()}
            target = staging / f"{table['table']}.masked.parquet"
            report = json.loads(native.mask_table(str(raw), str(target), json.dumps(rules), key))
            raw.unlink()
            record_tables.append(dict(
                table=table['table'], rows=report['rows'], rules=report['columns'],
                kept=table.get('keep') or {}, sample=table.get('sample'),
                follow=follow, where=table.get('where'),
            ))
        if out.exists():
            shutil.rmtree(out)
        out.mkdir(parents=True)
        for item in record_tables:
            shutil.move(staging / f"{item['table']}.masked.parquet",
                        out / f"{item['table']}.parquet")
        _duckdb(out, [t['table'] for t in record_tables])
        record = dict(
            sample=name, at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
            by=getpass.getuser(), source=dict(profile=value['source']['profile'], kind=kind),
            key_env=key_env, tables=record_tables,
        )
        (out / 'record.json').write_text(json.dumps(record, indent=2, default=str) + '\n')
        return record
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _duckdb(out: Path, tables: List[str]) -> None:
    try:
        import duckdb
    except ImportError:
        return
    connection = duckdb.connect(str(out / 'dev.duckdb'))
    try:
        for table in tables:
            schema, _, name = table.rpartition('.')
            if schema:
                connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            target = f'"{schema}"."{name}"' if schema else f'"{name}"'
            connection.execute(
                f'CREATE OR REPLACE TABLE {target} AS SELECT * FROM read_parquet(?)',
                [str(out / f'{table}.parquet')],
            )
    finally:
        connection.close()


def _reader(repo_path: str, profile: str, kind: str = 'postgres'):
    """reader(sql, params) for the profile's database: a pandas DataFrame of the rows."""
    from mage_ai.io.config import ConfigFileLoader, ConfigKey

    config = ConfigFileLoader(os.path.join(repo_path, 'io_config.yaml'), profile)

    def frame(cursor):
        import pandas as pd

        columns = [d[0] for d in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)

    if kind == 'duckdb':
        def read_duckdb(sql: str, params: List):
            import duckdb

            path = config[ConfigKey.DUCKDB_DATABASE]
            with duckdb.connect(path, read_only=True) as connection:
                return frame(connection.execute(sql, params or []))

        return read_duckdb
    if kind == 'mysql':
        from mage_ai.io.mysql import MySQL

        def read_mysql(sql: str, params: List):
            with MySQL.with_config(config) as loader:
                with loader.conn.cursor() as cursor:
                    cursor.execute(sql, params or None)
                    return frame(cursor)

        return read_mysql

    from mage_ai.io.postgres import Postgres

    def read_postgres(sql: str, params: List):
        with Postgres.with_config(config) as loader:
            with loader.conn.cursor() as cursor:
                cursor.execute(sql, params or None)
                return frame(cursor)

    return read_postgres
