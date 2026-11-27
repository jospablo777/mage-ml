"""
OpenLineage events of an exported service (lineage.rs): a run and its block attempts reach
the backend as the OpenLineage spec defines them (validated against its JSON schemas), with
the blocks' data between them as datasets and a frame's columns as its schema.
"""
import json
import textwrap
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from integration_tests.exported_services import export_service, run_service

SCHEMAS = Path(__file__).parent / 'schemas'
SCHEMA_URLS = {
    'OpenLineage-2-0-2.json': 'https://openlineage.io/spec/2-0-2/OpenLineage.json',
    'ParentRunFacet-1-1-0.json': 'https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json',
    'SchemaDatasetFacet-1-1-1.json':
        'https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json',
    'ErrorMessageRunFacet-1-0-1.json':
        'https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json',
}


def registry() -> Registry:
    resources = []
    for name, url in SCHEMA_URLS.items():
        contents = json.loads((SCHEMAS / name).read_text())
        resources.append((url, Resource.from_contents(contents)))
    return Registry().with_resources(resources)


def validate(event: dict, schemas: Registry) -> None:
    """The event against the run event schema and each facet against its own."""
    def check(value: dict, url: str) -> None:
        base, pointer = url.split('#')
        schema = {'$ref': url}
        Draft202012Validator(schema, registry=schemas).validate(value)
        assert base in SCHEMA_URLS.values() and pointer, url

    check(event, event['schemaURL'])
    for facet in event['run'].get('facets', {}).values():
        check(facet, facet['_schemaURL'])
    for dataset in event.get('inputs', []) + event.get('outputs', []):
        for facet in dataset.get('facets', {}).values():
            check(facet, facet['_schemaURL'])


class Backend:
    """A lineage backend on a local port: the events it received, in order."""

    def __init__(self):
        events = self.events = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                events.append((self.path, self.headers.get('Authorization'), json.loads(body)))
                self.send_response(201)
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = HTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f'http://127.0.0.1:{self.server.server_address[1]}'


def lineage_pipeline(project):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.create('lineage_orders', repo_path=project)

    def add(name, kind, code, upstream=()):
        block = Block.create(f'lineage_{name}', kind, project, language='python')
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    load = add('load', 'data_loader', '''
        import pandas as pd

        @data_loader
        def load(*args, **kwargs):
            return pd.DataFrame({'id': [1, 2, 3], 'amount': [10.5, 2.0, 7.25]})
    ''')
    add('totals', 'transformer', '''
        @transformer
        def totals(orders, *args, **kwargs):
            orders['total'] = orders['amount'] * 2
            return orders
    ''', [load])
    return pipeline


def test_a_runs_lineage_follows_the_openlineage_spec(mage_project, tmp_path, monkeypatch):
    backend = Backend()
    pipeline = lineage_pipeline(mage_project)
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        monkeypatch.setenv('OPENLINEAGE_URL', backend.url)
        monkeypatch.setenv('OPENLINEAGE_API_KEY', 'secret-key')
        monkeypatch.setenv('OPENLINEAGE_NAMESPACE', 'shop')

        run = run_service(directory, pipeline.uuid, tmp_path)
    finally:
        pipeline.delete()
        backend.server.shutdown()

    schemas = registry()
    events = [event for _, _, event in backend.events]
    assert {path for path, _, _ in backend.events} == {'/api/v1/lineage'}
    assert {auth for _, auth, _ in backend.events} == {'Bearer secret-key'}
    for event in events:
        validate(event, schemas)

    by_job = {}
    for event in events:
        by_job.setdefault(event['job']['name'], []).append(event)
    assert sorted(by_job) == ['lineage_orders', 'lineage_orders.lineage_load',
                              'lineage_orders.lineage_totals']
    # Each job: START, then COMPLETE, in order, under the run's id.
    for job, sequence in by_job.items():
        assert [e['eventType'] for e in sequence] == ['START', 'COMPLETE'], job
        assert {e['job']['namespace'] for e in sequence} == {'shop'}
    assert by_job['lineage_orders'][0]['run']['runId'] == run['id']
    totals = by_job['lineage_orders.lineage_totals'][1]
    assert totals['run']['facets']['parent']['run']['runId'] == run['id']
    assert totals['inputs'] == [{'namespace': 'shop', 'name': 'lineage_orders.lineage_load'}]
    [output] = totals['outputs']
    assert output['name'] == 'lineage_orders.lineage_totals'
    columns = [f['name'] for f in output['facets']['schema']['fields']]
    assert columns[:3] == ['id', 'amount', 'total']
    # The run's events come first and last.
    assert events[0]['job']['name'] == 'lineage_orders'
    assert events[-1]['job']['name'] == 'lineage_orders'


def test_a_sql_blocks_tables_are_named_as_openlineage_names_them(
    mage_project, tmp_path, monkeypatch,
):
    import duckdb

    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    database = tmp_path / 'shop.duckdb'
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "CREATE TABLE customers AS SELECT * FROM (VALUES (1, 'north'), (2, 'south')) "
            't(id, region)'
        )
    pipeline = Pipeline.create('lineage_tables', repo_path=mage_project)
    load = Block.create('lineage_tables_load', 'data_loader', mage_project, language='python')
    Path(load.file_path).write_text(textwrap.dedent('''
        import pandas as pd

        @data_loader
        def load(*args, **kwargs):
            return pd.DataFrame({'id': [1, 2], 'amount': [10.5, 2.0]})
    '''))
    pipeline.add_block(load)
    regions = Block.create(
        'lineage_tables_regions', 'transformer', mage_project, language='sql',
        configuration=dict(data_provider='duckdb', data_provider_profile='duckdb',
                           data_provider_table='orders_regions'),
    )
    Path(regions.file_path).write_text(
        'SELECT o.id, o.amount, c.region FROM {{ df_1 }} o JOIN customers c ON c.id = o.id')
    pipeline.add_block(regions, upstream_block_uuids=[load.uuid])
    backend = Backend()
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        monkeypatch.setenv('OPENLINEAGE_URL', backend.url)
        monkeypatch.setenv('MAGE_TEST_DUCKDB_DATABASE', str(database))

        run_service(directory, pipeline.uuid, tmp_path)
    finally:
        pipeline.delete()
        backend.server.shutdown()

    schemas = registry()
    events = [event for _, _, event in backend.events]
    for event in events:
        validate(event, schemas)
    [done] = [e for e in events if e['eventType'] == 'COMPLETE'
              and e['job']['name'] == 'lineage_tables.lineage_tables_regions']
    namespace = f'duckdb://{database}'
    assert dict(namespace=namespace, name='main.customers') in done['inputs']
    assert dict(namespace=namespace, name='main.orders_regions') in done['outputs']
    # Its upstream block is MageML's dataset, not a table.
    names = [d['name'] for d in done['inputs']]
    assert 'lineage_tables.lineage_tables_load' in names
