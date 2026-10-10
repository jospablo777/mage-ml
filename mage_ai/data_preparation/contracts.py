"""
Data contracts: declared rules for a block's output (columns, types, missing values,
ranges, allowed values, patterns, unique keys, row counts), checked over the whole
stored output every time the block runs, wherever it runs: the notebook, triggers,
backfills, retries and block fusion.

A contract is a YAML file in the project's contracts folder:

    # contracts/customer_scores.yaml
    version: 1.2.0
    owner: ml-engineering
    columns:
      customer_id: {type: integer, nullable: false}
      score: {type: float, nullable: false, min: 0, max: 1}
      segment: {type: string, values: [new, active, churned]}
    unique: [customer_id]

and a block uses it in its configuration (block settings, or metadata.yaml):

    configuration:
      contract: customer_scores                  # or:
      contract: {name: customer_scores, enforcement: warn, version: '1'}

The check runs with the block's tests, after its output is stored, in the Rust engine of
ColumnAtlas (rust/column_atlas/src/contract.rs). With enforcement `fail` (the default) a
violation fails the block like a failing test, so downstream blocks, exporters included,
do not run; `warn` reports it and continues; `off` skips the check. Each check's report
is saved with the pipeline's variables and printed in the block's output and logs.
"""
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

CONTRACTS_FOLDER = 'contracts'
REPORTS_FOLDER = '.contract_reports'
ENFORCEMENTS = ('fail', 'warn', 'off')
EXAMPLES = int(os.getenv('MAGE_CONTRACT_EXAMPLES') or 10)
_NAME = re.compile(r'^[A-Za-z0-9][\w.\-]{0,127}$')
_BINDING_KEYS = {'name', 'enforcement', 'output', 'version'}
_OUTPUT = re.compile(r'^output_\d{1,6}$')


class ContractError(Exception):
    pass


@dataclass(frozen=True)
class Binding:
    name: str
    enforcement: str = 'fail'
    output: str = 'output_0'
    # The major version (or major.minor) the block was written for.
    version: Optional[str] = None


def binding_of(block) -> Optional[Binding]:
    """The contract the block's output must meet, None when it has none."""
    config = (getattr(block, 'configuration', None) or {}).get('contract')
    if not config:
        return None
    if isinstance(config, str):
        config = dict(name=config)
    if not isinstance(config, dict):
        raise ContractError(
            f'The contract of block {block.uuid} must be a contract name or a mapping with '
            'name, enforcement, output and version.'
        )
    unknown = set(config) - _BINDING_KEYS
    if unknown:
        raise ContractError(
            f'The contract of block {block.uuid} has unknown settings: '
            f'{", ".join(sorted(unknown))}; the settings are name, enforcement, output and '
            'version.'
        )
    name = config.get('name')
    if not isinstance(name, str) or not _NAME.match(name):
        raise ContractError(f'The contract of block {block.uuid} needs a valid name.')
    enforcement = str(config.get('enforcement') or 'fail')
    if enforcement not in ENFORCEMENTS:
        raise ContractError(
            f'The contract enforcement of block {block.uuid} is {enforcement}; it must be '
            f'one of {", ".join(ENFORCEMENTS)}.'
        )
    output = str(config.get('output') or 'output_0')
    if not _OUTPUT.match(output):
        raise ContractError(
            f'The contract output of block {block.uuid} must be output_0, output_1 and so on.'
        )
    version = config.get('version')
    return Binding(
        name=name,
        enforcement=enforcement,
        output=output,
        version=str(version) if version not in (None, '') else None,
    )


def contracts_dir(repo_path: str) -> Path:
    return Path(repo_path) / CONTRACTS_FOLDER


def contract_path(name: str, repo_path: str) -> Path:
    if not _NAME.match(name or ''):
        raise ContractError(f'{name!r} is not a valid contract name.')
    folder = contracts_dir(repo_path)
    for suffix in ('.yaml', '.yml'):
        path = folder / f'{name}{suffix}'
        if path.is_file():
            return path
    raise ContractError(
        f'Contract {name} does not exist; create {CONTRACTS_FOLDER}/{name}.yaml in the project.'
    )


def _columns(name: str, columns: Any) -> List[Dict]:
    """Columns as the engine takes them: a list of rules, each with its name."""
    if columns is None:
        return []
    if isinstance(columns, list):
        return columns
    if not isinstance(columns, dict):
        raise ContractError(f'The columns of contract {name} must be a mapping of column rules.')
    rules = []
    for column, rule in columns.items():
        if rule is None:
            rule = {}
        elif isinstance(rule, str):
            # `score: float` is short for `score: {type: float}`.
            rule = dict(type=rule)
        elif not isinstance(rule, dict):
            raise ContractError(
                f'The rules of column {column} in contract {name} must be a mapping or a type.'
            )
        rules.append(dict(rule, name=str(column)))
    return rules


def _json_safe(value: Any) -> Any:
    """YAML dates in min and max become ISO text, as the engine reads them."""
    from datetime import date, datetime

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def load_contract(name: str, repo_path: str) -> Dict:
    path = contract_path(name, repo_path)
    try:
        document = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as error:
        raise ContractError(f'Contract {name} is not valid YAML: {error}')
    if not isinstance(document, dict):
        raise ContractError(f'Contract {name} must be a mapping.')
    declared = document.get('name')
    if declared is not None and str(declared) != name:
        raise ContractError(
            f'The file {path.name} names the contract {declared}; rename the file or the '
            'contract so they match.'
        )
    contract = dict(document, name=name, columns=_columns(name, document.get('columns')))
    if contract.get('version') is not None:
        contract['version'] = str(contract['version'])
    if isinstance(contract.get('unique'), str):
        contract['unique'] = [contract['unique']]
    contract = _json_safe(contract)
    import column_atlas_native

    try:
        column_atlas_native.check_contract(json.dumps(contract))
    except ValueError as error:
        raise ContractError(f'Contract {name}: {error}')
    return contract


def list_contracts(repo_path: str) -> List[Dict]:
    """Every contract of the project, with the error of each one that does not load."""
    folder = contracts_dir(repo_path)
    if not folder.is_dir():
        return []
    contracts = []
    for path in sorted(folder.iterdir()):
        if path.suffix not in ('.yaml', '.yml') or not _NAME.match(path.stem):
            continue
        try:
            contract = load_contract(path.stem, repo_path)
            contracts.append(dict(
                name=path.stem,
                version=contract.get('version'),
                owner=contract.get('owner'),
                description=contract.get('description'),
                columns=[c.get('name') for c in contract['columns']],
            ))
        except ContractError as error:
            contracts.append(dict(name=path.stem, error=str(error)))
    return contracts


def _version_matches(version: Optional[str], pin: str) -> bool:
    if not version:
        return False
    return version == pin or version.startswith(pin + '.')


def _write_frame(frame: Any, directory: str) -> Optional[str]:
    """A pandas or Polars DataFrame as a Parquet file in directory; None for other values."""
    import pandas as pd
    import polars as pl

    path = os.path.join(directory, 'output.parquet')
    if isinstance(frame, pl.DataFrame):
        frame.write_parquet(path)
        return path
    if isinstance(frame, pd.DataFrame):
        import pyarrow as pa
        import pyarrow.parquet as pq

        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path)
        return path
    return None


def _stored_file(block, binding: Binding, execution_partition, dynamic_block_uuid):
    """The output's Parquet file as Mage stored it, None when it is not a local file."""
    from mage_ai.column_atlas.sources import DATAFRAME_FILE, EXPLORABLE_TYPES
    from mage_ai.data_preparation.storage.local_storage import LocalStorage

    try:
        variable = block.pipeline.variable_manager.get_variable_object(
            block.pipeline.uuid,
            dynamic_block_uuid or block.uuid,
            binding.output,
            partition=execution_partition,
        )
    except Exception:
        return None, None
    if variable.variable_type not in EXPLORABLE_TYPES:
        return None, variable
    if not isinstance(variable.storage, LocalStorage):
        return None, variable
    path = os.path.join(variable.variable_path, DATAFRAME_FILE)
    return (path if os.path.isfile(path) else None), variable


def check_output(
    block,
    execution_partition: Optional[str] = None,
    dynamic_block_uuid: Optional[str] = None,
    outputs: Optional[List[Any]] = None,
) -> Optional[Tuple[Binding, Dict]]:
    """
    Checks the block's output against its contract; returns the binding and the report,
    None when the block has no contract or its enforcement is off.
    """
    binding = binding_of(block)
    if binding is None or binding.enforcement == 'off':
        return None
    repo_path = getattr(block, 'repo_path', None) or block.pipeline.repo_path
    contract = load_contract(binding.name, repo_path)
    if binding.version and not _version_matches(contract.get('version'), binding.version):
        raise ContractError(
            f'Block {block.uuid} expects version {binding.version} of contract {binding.name}, '
            f'which is at version {contract.get("version") or "(none)"}. Update the block for '
            'the new version, then change the version in its contract setting.'
        )

    import column_atlas_native

    with tempfile.TemporaryDirectory(prefix='mage_contract_') as directory:
        path, variable = (None, None)
        if block.pipeline is not None:
            path, variable = _stored_file(
                block, binding, execution_partition, dynamic_block_uuid,
            )
        if path is None:
            frame = None
            index = int(binding.output.split('_')[1])
            if outputs is not None and index < len(outputs):
                frame = outputs[index]
            elif variable is not None:
                frame = variable.read_data()
            path = _write_frame(frame, directory) if frame is not None else None
            if path is None:
                raise ContractError(
                    f'Contract {binding.name} applies to DataFrame outputs; {binding.output} of '
                    f'block {block.uuid} is '
                    + (type(frame).__name__ if frame is not None else 'not stored') + '.'
                )
        try:
            report = json.loads(
                column_atlas_native.validate_contract(path, json.dumps(contract), EXAMPLES),
            )
        except ValueError as error:
            raise ContractError(f'Contract {binding.name}: {error}')
    report['enforcement'] = binding.enforcement
    report['output'] = binding.output
    _save_report(block, execution_partition, dynamic_block_uuid, report)
    return binding, report


def report_path(block, execution_partition=None, dynamic_block_uuid=None) -> Optional[str]:
    from mage_ai.shared.utils import clean_name

    if block.pipeline is None:
        return None
    manager = block.pipeline.variable_manager
    return os.path.join(
        manager.pipeline_path(block.pipeline.uuid),
        REPORTS_FOLDER,
        execution_partition or '',
        f'{clean_name(dynamic_block_uuid or block.uuid)}.json',
    )


def _save_report(block, execution_partition, dynamic_block_uuid, report: Dict) -> None:
    path = report_path(block, execution_partition, dynamic_block_uuid)
    if path is None:
        return
    storage = block.pipeline.variable_manager.storage
    storage.makedirs(os.path.dirname(path), exist_ok=True)
    storage.write_json_file(path, report)


def read_report(block, execution_partition=None, dynamic_block_uuid=None) -> Optional[Dict]:
    path = report_path(block, execution_partition, dynamic_block_uuid)
    if path is None:
        return None
    storage = block.pipeline.variable_manager.storage
    if not storage.path_exists(path):
        return None
    return storage.read_json_file(path)


def summary(binding: Binding, report: Dict) -> str:
    """The report as text for the block's output and logs."""
    version = f' {report["version"]}' if report.get('version') else ''
    title = f'Contract {report["contract"]}{version} on {binding.output}'
    rows = report.get('rows', 0)
    if report.get('passed'):
        return f'{title}: passed ({rows:,} rows checked).'
    violations = report.get('violations') or []
    lines = [
        f'{title}: {len(violations)} rule'
        f'{"" if len(violations) == 1 else "s"} broken ({rows:,} rows checked).'
    ]
    for violation in violations:
        line = f'- {violation["message"]}'
        if violation.get('values'):
            line += '; values: ' + ', '.join(violation['values'])
        if violation.get('examples'):
            line += '; rows: ' + ', '.join(str(row) for row in violation['examples'])
        lines.append(line)
    return '\n'.join(lines)


def draft(block, name: str, execution_partition: Optional[str] = None) -> str:
    """A contract draft (YAML) from the block's stored output."""
    import column_atlas_native

    binding = Binding(name=name)
    path, variable = _stored_file(block, binding, execution_partition, None)
    with tempfile.TemporaryDirectory(prefix='mage_contract_') as directory:
        if path is None and variable is not None:
            path = _write_frame(variable.read_data(), directory)
        if path is None:
            raise ContractError(
                f'Block {block.uuid} has no stored DataFrame output; run it first.'
            )
        document = json.loads(column_atlas_native.infer_contract(path, name))
    columns = {}
    for column in document.pop('columns'):
        rule = dict(type=column['type'])
        if not column['nullable']:
            rule['nullable'] = False
        columns[column['name']] = rule
    document['columns'] = columns
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=True)
