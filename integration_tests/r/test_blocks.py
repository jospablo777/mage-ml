"""
R blocks run with R 4.6 and an rv environment: what crosses between Python and R, errors,
tests, timeouts and the checks of the environment.
"""
import datetime as dt
import decimal
import shutil
import time

import numpy as np
import pandas as pd
import polars as pl
import pytest

from integration_tests.data import r_dataset
from mage_ai.data_preparation.models.block.r import RBlockError, execute_r_code, runtime
from mage_ai.data_preparation.models.constants import BlockType

IDENTITY = 'transformer(function(df_1, ...) df_1)'


def transform(code, *inputs, **options):
    return execute_r_code(BlockType.TRANSFORMER, code, input_vars=list(inputs), **options)


def output(code, *inputs, **options):
    return transform(code, *inputs, **options).outputs[0]


@pytest.fixture(scope='module')
def source():
    return r_dataset.source_frame()


@pytest.fixture(scope='module')
def round_trip(source):
    return output(IDENTITY, source)


def test_values_round_trip(source, round_trip):
    assert r_dataset.round_trip_mismatches(source, round_trip) == {}


def test_types_after_the_round_trip(round_trip):
    dtypes = {name: str(dtype) for name, dtype in round_trip.dtypes.items()}

    assert dtypes['id'] == 'Int64'
    assert dtypes['c_int'] == 'Int64'
    assert dtypes['c_big'] == 'Int64'
    assert dtypes['c_double'] == 'float64'
    assert dtypes['c_bool'] == 'boolean'
    assert dtypes['c_text'] == 'str'
    assert dtypes['c_category'] == 'category'
    assert dtypes['c_date'] == 'date32[day][pyarrow]'
    assert dtypes['c_ts'] == 'datetime64[us]'
    assert dtypes['c_tstz'] == 'datetime64[us, America/New_York]'
    assert dtypes['c_duration'] == 'timedelta64[us]'
    assert dtypes['c_time'] == 'time64[us][pyarrow]'
    assert dtypes['c_decimal'] == 'float64'
    assert list(round_trip['c_category'].cat.categories) == r_dataset.CATEGORIES


R_TYPES = '''
transformer(function(df_1) {
  classes <- vapply(df_1, function(x) class(x)[[1]], character(1))
  tibble::tibble(column = names(classes), class = unname(classes))
})
'''


def test_r_types(source):
    classes = output(R_TYPES, source).set_index('column')['class'].to_dict()

    assert classes == {
        'id': 'integer',
        'c_int': 'integer',
        'c_big': 'integer64',
        'c_double': 'numeric',
        'c_bool': 'logical',
        'c_text': 'character',
        'c_category': 'factor',
        'c_date': 'Date',
        'c_ts': 'POSIXct',
        'c_tstz': 'POSIXct',
        'c_duration': 'difftime',
        'c_time': 'hms',
        'c_decimal': 'numeric',
        'c_int_list': 'arrow_list',
        'c_text_list': 'arrow_list',
        'c_struct': 'list',
        'c_binary': 'arrow_binary',
        'c_uuid': 'character',
    }


def test_tidyverse_transformations(source, r_project_dir):
    """The R transformer of the r_types pipeline, run on its own."""
    code = (r_project_dir.parent / 'project' / 'transformers' / 'r_types_transform.r').read_text()

    run = transform(code, source, global_vars=dict(multiplier=2.5, prefix="it's "))

    result = run.outputs[0]
    assert r_dataset.round_trip_mismatches(source, result) == {}
    assert r_dataset.derived_mismatches(source, result, 2.5, "it's ") == {}
    assert str(result['r_big_plus_one'].dtype) == 'Int64'
    assert [(t['name'], t['passed']) for t in run.tests] == [
        ('r_types', True), ('pointblank_checks', True),
    ]


def test_pointblank_failures_fail_the_test():
    code = """
suppressPackageStartupMessages(library(pointblank))
transformer(function(df_1) df_1)
test(function(output) output |> col_vals_not_null(id) |> rows_distinct(id))
"""
    run = transform(code, pd.DataFrame({'id': [1, None, 1]}))

    assert run.tests[0]['passed'] is False
    assert 'should not have been NULL' in run.tests[0]['message']


def test_polars_inputs(source):
    frame = pl.from_pandas(source[['id', 'c_big', 'c_text', 'c_ts', 'c_int_list']])

    result = output(IDENTITY, frame.lazy())

    assert r_dataset.values(result['c_big']) == r_dataset.values(source['c_big'])
    assert r_dataset.values(result['c_text']) == r_dataset.values(source['c_text'])
    assert r_dataset.values(result['c_ts']) == r_dataset.values(source['c_ts'])
    assert r_dataset.values(result['c_int_list']) == r_dataset.values(source['c_int_list'])


def test_several_inputs_and_values():
    code = '''
transformer(function(df_1, df_2, settings) {
  stopifnot(identical(df_2, df_1))
  df_1$label <- paste(settings$name, settings$values[[2]])
  df_1
})
'''
    frame = pd.DataFrame({'id': [1, 2]})

    result = output(code, frame, frame, {'name': 'n', 'values': [1, 2]})

    assert result['label'].tolist() == ['n 2', 'n 2']


def test_values_other_than_frames():
    code = 'transformer(function(df_1) list(rows = nrow(df_1), names = names(df_1)))'

    assert output(code, pd.DataFrame({'a': [1], 'b': [2]})) == {'rows': 1, 'names': ['a', 'b']}
    assert transform('transformer(function(df_1) NULL)', pd.DataFrame({'a': [1]})).outputs == []


def test_empty_frames():
    frame = pd.DataFrame({
        'id': pd.array([], dtype='Int64'),
        'text': pd.Series([], dtype='str'),
        'at': pd.Series([], dtype='datetime64[us]'),
    })

    result = output(IDENTITY, frame)

    assert len(result) == 0
    assert {name: str(dtype) for name, dtype in result.dtypes.items()} == {
        'id': 'Int64', 'text': 'str', 'at': 'datetime64[us]',
    }


def test_global_variables_cross_as_json():
    code = 'data_loader(function() global_vars)'
    variables = dict(
        execution_date=dt.datetime(2026, 10, 9, 12, 0),
        quote="it's \"quoted\" \\ ñ",
        big=2**53 + 1,
        nested={'a': [1, None], 'b': {'c': True}},
        nothing=None,
    )

    result = execute_r_code(BlockType.DATA_LOADER, code, global_vars=variables).outputs[0]

    assert result == dict(
        execution_date='2026-10-09T12:00:00',
        quote="it's \"quoted\" \\ ñ",
        big=2**53 + 1,
        nested={'a': [1, None], 'b': {'c': True}},
        nothing=None,
    )


def test_r_errors_name_the_block_lines():
    code = '''transformer(function(df_1) {
  validate(df_1)
})

validate <- function(df) {
  if (nrow(df) < 5) stop("expected 5 rows, got ", nrow(df))
}
'''

    with pytest.raises(RBlockError) as error:
        transform(code, pd.DataFrame({'a': [1]}), block_uuid='checker')

    assert str(error.value) == (
        'R block checker failed: expected 5 rows, got 1\n\n'
        'R calls:\n'
        'block.R#2: validate(df_1)\n'
        'block.R#6: stop("expected 5 rows, got ", nrow(df))'
    )


def test_syntax_errors():
    with pytest.raises(RBlockError, match='unexpected'):
        transform('transformer(function(df_1) {', pd.DataFrame({'a': [1]}))


def test_packages_outside_the_environment_are_not_found():
    """Blocks see the rv library and R's base packages, not site or user libraries."""
    code = '''
data_loader(function() {
  list(paths = .libPaths(), has_missing = requireNamespace("notinstalled", quietly = TRUE))
})
'''
    result = execute_r_code(BlockType.DATA_LOADER, code).outputs[0]

    paths = result['paths']
    assert len(paths) == 3
    assert paths[0].startswith(str(runtime.r_config().project_dir))
    assert 'mageml-' in paths[1]
    assert result['has_missing'] is False


def test_a_missing_package_names_the_rv_command():
    code = 'library(notinstalled)\ntransformer(function(df_1) df_1)'

    with pytest.raises(RBlockError, match="there is no package called .notinstalled."):
        transform(code, pd.DataFrame({'a': [1]}))


def test_timeout(monkeypatch):
    monkeypatch.setenv('MAGE_R_TIMEOUT', '2')
    started = time.monotonic()

    with pytest.raises(TimeoutError, match='longer than 2 seconds'):
        transform('transformer(function(df_1) { Sys.sleep(60); df_1 })', pd.DataFrame({'a': [1]}))

    assert time.monotonic() - started < 15


def test_output_is_printed_as_it_comes(capsys):
    code = '''
transformer(function(df_1) {
  print("from print")
  message("from message")
  warning("from warning")
  cat("ñ from cat\\n")
  df_1
})
'''
    transform(code, pd.DataFrame({'a': [1]}))

    printed = capsys.readouterr().out
    for text in ('from print', 'from message', 'Warning', 'from warning', 'ñ from cat'):
        assert text in printed


def test_tests_report_results():
    code = '''
transformer(function(df_1) df_1)
test(function(output) stopifnot(nrow(output) == 2))
test(function(output) stopifnot("ids must be unique" = !anyDuplicated(output$id)))
'''
    run = transform(code, pd.DataFrame({'id': [1, 1]}))

    assert run.tests == [
        {'name': 'test_1', 'passed': True, 'message': None},
        {'name': 'test_2', 'passed': False, 'message': 'ids must be unique'},
    ]


def test_out_of_sync_environments_are_refused(r_project_dir, tmp_path, monkeypatch):
    project = tmp_path / 'r'
    shutil.copytree(r_project_dir, project, ignore=shutil.ignore_patterns('library'))
    toml = project / 'rproject.toml'
    toml.write_text(toml.read_text().replace('    "DBI",\n', '    "DBI",\n    "glue",\n'))
    monkeypatch.setenv('MAGE_R_PROJECT_DIR', str(project))

    with pytest.raises(runtime.REnvironmentError, match='not synced with rv.lock'):
        transform(IDENTITY, pd.DataFrame({'a': [1]}))


def test_the_r_version_must_match(r_project_dir, tmp_path, monkeypatch):
    project = tmp_path / 'r'
    shutil.copytree(r_project_dir, project, ignore=shutil.ignore_patterns('library'))
    toml = project / 'rproject.toml'
    toml.write_text(toml.read_text().replace('r_version = "4.6"', 'r_version = "4.5"'))
    monkeypatch.setenv('MAGE_R_PROJECT_DIR', str(project))

    with pytest.raises(runtime.REnvironmentError, match='is for R 4.5, but .* runs R 4.6'):
        transform(IDENTITY, pd.DataFrame({'a': [1]}))


def test_status_reports_a_working_environment(r_project_dir):
    report = runtime.status(runtime.r_config())

    assert report['problems'] == []
    assert report['details']['R version of the environment'] == '4.6'


def test_large_frames_cross_quickly():
    rows = 1_000_000
    frame = pd.DataFrame({
        'id': np.arange(rows, dtype='int64'),
        'x': np.random.default_rng(1).random(rows),
        'text': pd.Series(np.repeat(['a', 'bb', 'ccc', 'dddd'], rows // 4), dtype='str'),
    })
    transform(IDENTITY, frame.head(10))

    started = time.monotonic()
    result = output('transformer(function(df_1) { df_1$y <- df_1$x * 2; df_1 })', frame)
    elapsed = time.monotonic() - started

    assert len(result) == rows
    assert result['id'].iloc[-1] == rows - 1
    assert elapsed < 10, elapsed


def test_decimal_precision_warning(capsys):
    frame = pd.DataFrame({'amount': [decimal.Decimal('12345678901234567890.12')]})

    output(IDENTITY, frame, block_uuid='money')

    assert 'Column amount is decimal128(22, 2)' in capsys.readouterr().out


def test_empty_globals_are_a_named_list():
    """jsonlite reads [] as an unnamed list; Mage writes {} when there are no variables."""
    code = 'data_loader(function() list(is_list = is.list(global_vars), n = length(global_vars)))'

    result = execute_r_code(BlockType.DATA_LOADER, code).outputs[0]

    assert result == {'is_list': True, 'n': 0}


def test_custom_blocks():
    """R custom blocks take any inputs and return any value, as Python's do."""
    from mage_ai.data_preparation.models.constants import BlockType
    from mage_ai.data_preparation.templates.template import fetch_template_source

    code = fetch_template_source(BlockType.CUSTOM, {}, language='r')
    assert '#* @custom' in code
    run = execute_r_code(
        BlockType.CUSTOM, code, input_vars=[pd.DataFrame({'x': [1, 2]}), {'a': 1}],
    )
    assert run.outputs[0]['inputs'] == 2

    run = execute_r_code(
        BlockType.CUSTOM,
        '#* @custom\nsummary <- function(df_1, ...) {\n  data.frame(rows = nrow(df_1))\n}\n',
        input_vars=[pd.DataFrame({'x': [1, 2, 3]})],
    )
    assert run.outputs[0]['rows'].tolist() == [3]
