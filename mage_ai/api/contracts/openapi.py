"""
Today's API as OpenAPI (ROADMAP P0): the routes and operations the Python server answers,
with response schemas recorded from its live responses. The Rust gateway is compared with
it (no response differences on the recorded API).

Paths come from three places:
    - every resource with a policy: /api/{resource} and /api/{resource}/{id}, with the
      operations its resource class implements (collection, create, member, update, delete);
    - the parent and child pairs the frontend calls (RESOURCES_PAIRS_ARRAY in
      frontend/api/index.ts): /api/{parent}/{parent_id}/{resource}[/{id}[/{grandchild}]];
    - the routes make_app registers with their own handlers (/api/runs, /api/events, ...).

Response schemas: scripts/contracts/record.py seeds a scratch project through the API (a
pipeline with two blocks, a tag, a trigger, a run, a backfill, a secret, a role and a Python
worker folder), calls every list and detail it can reach,
and infers each resource's schema from the JSON it got: the union of the types seen for
each attribute. A resource it could not reach keeps its presenter's attribute names with no
type, and is listed under x-mage-unrecorded with the reason.
"""
import json
import os
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import inflection

from mage_ai.api.operations.constants import OperationType

API_DIR = os.path.dirname(os.path.dirname(__file__))
FRONTEND_INDEX = os.path.join(
    os.path.dirname(API_DIR), 'frontend', 'api', 'index.ts',
)
BASE_CLASSES = ('AsyncBaseResource', 'BaseResource', 'DatabaseResource', 'GenericResource')
# Operation: the resource class methods that implement it, and its HTTP method and path.
OPERATIONS = {
    OperationType.LIST: (('collection', 'process_collection'), 'get', False),
    OperationType.CREATE: (('create', 'process_create'), 'post', False),
    OperationType.DETAIL: (('member', 'process_member'), 'get', True),
    OperationType.UPDATE: (('update', 'process_update'), 'put', True),
    OperationType.DELETE: (('delete', 'process_delete'), 'delete', True),
}
# The routes make_app gives their own handlers; the generic /api/{resource} routes and the
# pages are left out (the generic ones are the resource paths).
GENERIC_HANDLERS = (
    'ApiChildDetailHandler',
    'ApiChildListHandler',
    'ApiResourceDetailHandler',
    'ApiResourceListHandler',
)
HTTP_METHODS = ('delete', 'get', 'patch', 'post', 'put')


def resource_classes() -> Dict[str, type]:
    """{resource name: resource class} for every resource with a policy."""
    import importlib

    classes = {}
    policies = set(os.listdir(os.path.join(API_DIR, 'policies')))
    for file in sorted(os.listdir(os.path.join(API_DIR, 'resources'))):
        if not file.endswith('Resource.py'):
            continue
        name = file[:-len('.py')]
        if name in BASE_CLASSES or name == 'Resource':
            continue
        if f"{name[:-len('Resource')]}Policy.py" not in policies:
            continue
        module = importlib.import_module(f'mage_ai.api.resources.{name}')
        cls = getattr(module, name)
        classes[cls.resource_name()] = cls
    return classes


def _implemented(cls: type, method: str) -> bool:
    from mage_ai.api.resources.BaseResource import BaseResource

    for klass in cls.__mro__:
        if method in klass.__dict__:
            return klass is not BaseResource and klass.__name__ not in ('GenericResource',)
    return False


def operations(cls: type) -> List[str]:
    """The operations a resource class implements, in OPERATIONS order."""
    return [
        str(operation) for operation, (methods, _, _) in OPERATIONS.items()
        if any(_implemented(cls, method) for method in methods)
    ]


def frontend_pairs(path: str = FRONTEND_INDEX) -> List[Tuple[str, ...]]:
    """The (resource, parent[, grandchild]) entries of RESOURCES_PAIRS_ARRAY with a parent."""
    text = open(path).read()
    names = dict(re.findall(r"export const (\w+)(?::\s*'[\w-]+')?\s*=\s*'([\w-]+)';", text))
    array = text.split('const RESOURCES_PAIRS_ARRAY', 1)[1].split('];', 1)[0]
    pairs = []
    for entry in re.findall(r'\[([A-Z_,\s]+)\]', array):
        keys = [names[key.strip()] for key in entry.split(',') if key.strip()]
        if len(keys) >= 2:
            pairs.append(tuple(keys))
    return pairs


def handler_routes() -> List[Tuple[str, List[str], str]]:
    """(path, methods, handler) for the /api routes make_app gives their own handlers."""
    from mage_ai.server.server import make_app

    app = make_app()
    routes = []
    for rule in app.wildcard_router.rules:
        handler = rule.target
        if not isinstance(handler, type) or handler.__name__ in GENERIC_HANDLERS:
            continue
        path = _openapi_path(rule.matcher.regex.pattern)
        if not path.startswith('/api/'):
            continue
        # The methods the handler defines; RequestHandler's own answer 405.
        methods = tuple(
            method for method in HTTP_METHODS
            if getattr(handler, method).__qualname__.split('.')[0] != 'RequestHandler'
        )
        routes.append((path, methods, handler.__name__))
    return sorted(set(routes))


def _openapi_path(pattern: str) -> str:
    """A Tornado route pattern as an OpenAPI path: (?P<name>...) becomes {name}."""
    path = pattern.rstrip('$')
    path = re.sub(r'\(\?P<(\w+)>(?:[^()]|\([^()]*\))*\)', r'{\1}', path)
    path = path.replace('(?:es)?', '')
    path = re.sub(r'\(\.\*\)', '{path}', path)
    return path.replace('\\', '')


def _parameters(path: str) -> List[Dict]:
    return [
        dict(**{'in': 'path'}, name=name, required=True, schema=dict(type='string'))
        for name in re.findall(r'{(\w+)}', path)
    ]


def _schema_name(resource: str) -> str:
    return inflection.camelize(inflection.singularize(resource))


def _envelope(resource: str, many: bool) -> Dict:
    ref = {'$ref': f'#/components/schemas/{_schema_name(resource)}'}
    if many:
        return dict(type='object', properties={
            resource: dict(type='array', items=ref),
            'metadata': dict(type='object'),
        })
    return dict(type='object', properties={inflection.singularize(resource): ref})


def _operation(resource: str, operation: str, parent: Optional[str] = None) -> Dict:
    many = operation == str(OperationType.LIST)
    body = {}
    if operation in (str(OperationType.CREATE), str(OperationType.UPDATE)):
        body = dict(requestBody=dict(content={'application/json': dict(schema=dict(
            type='object',
            properties={inflection.singularize(resource): dict(type='object')},
        ))}))
    summary = f'{operation} {resource}' + (f' of a {inflection.singularize(parent)}'
                                           if parent else '')
    return dict(
        operationId=f'{operation}_{resource}' + (f'_of_{parent}' if parent else ''),
        responses={'200': dict(
            content={'application/json': dict(schema=_envelope(resource, many))},
            description='OK; an error is {"error": {"code", "message", "type"}} with 200.',
        )},
        summary=summary,
        tags=[resource],
        **body,
    )


def _add(paths: Dict, path: str, method: str, operation: Dict) -> None:
    item = paths.setdefault(path, {})
    if method not in item:
        item[method] = operation
        params = _parameters(path)
        if params:
            item['parameters'] = params


def static_spec(
    classes: Dict[str, type] = None,
    pairs: Iterable[Tuple[str, ...]] = None,
    routes: Iterable[Tuple[str, Tuple[str, ...], str]] = None,
) -> Dict:
    """The paths and operations, from the code alone (no responses)."""
    classes = resource_classes() if classes is None else classes
    pairs = frontend_pairs() if pairs is None else pairs
    routes = handler_routes() if routes is None else routes
    paths: Dict[str, Dict] = {}
    implemented = {name: operations(cls) for name, cls in classes.items()}
    for resource, ops in implemented.items():
        for operation in ops:
            _, method, member = OPERATIONS[operation]
            path = f'/api/{resource}' + ('/{id}' if member else '')
            _add(paths, path, method, _operation(resource, operation))
    for pair in pairs:
        resource, parent = pair[0], pair[1]
        if resource not in implemented:
            continue
        if len(pair) == 3:
            grandchild = pair[2]
            path = f'/api/{parent}/{{parent_id}}/{resource}/{{id}}/{grandchild}'
            _add(paths, path, 'get', _operation(grandchild, str(OperationType.LIST), resource))
            continue
        for operation in implemented[resource]:
            _, method, member = OPERATIONS[operation]
            path = f'/api/{parent}/{{parent_id}}/{resource}' + ('/{id}' if member else '')
            _add(paths, path, method, _operation(resource, operation, parent))
    for path, methods, handler in routes:
        for method in methods:
            _add(paths, path, method, dict(
                operationId=f'{method}_{handler}',
                responses={'200': dict(description='OK')},
                summary=f'{handler}',
                tags=['handlers'],
            ))
    schemas = {}
    for resource, cls in classes.items():
        presenter = cls.presenter_class()
        schemas[_schema_name(resource)] = dict(
            properties={name: {} for name in sorted(presenter.default_attributes or [])},
            type='object',
        )
    return dict(
        components=dict(schemas=dict(sorted(schemas.items()))),
        info=dict(
            description='Recorded from the Python server by scripts/contracts/record.py.',
            title='MageML API',
            version='recorded',
        ),
        openapi='3.1.0',
        paths=dict(sorted(paths.items())),
        **{'x-mage-resources': {
            resource: dict(operations=implemented[resource], schema=_schema_name(resource))
            for resource in sorted(classes)
        }},
    )


def skeleton(spec: Dict) -> Dict[str, List[str]]:
    """Path to methods: what the static part decides, without the recorded schemas."""
    return {
        path: sorted(method for method in item if method in HTTP_METHODS)
        for path, item in spec['paths'].items()
    }


def schema_of(value: Any) -> Dict:
    """The JSON schema of a value, as recorded: types, properties, items."""
    if value is None:
        return dict(type='null')
    if isinstance(value, bool):
        return dict(type='boolean')
    if isinstance(value, int):
        return dict(type='integer')
    if isinstance(value, float):
        return dict(type='number')
    if isinstance(value, str):
        return dict(type='string')
    if isinstance(value, list):
        items: Dict = {}
        for item in value:
            items = merge(items, schema_of(item))
        return dict(items=items, type='array')
    if isinstance(value, dict):
        return dict(
            properties={str(key): schema_of(item) for key, item in sorted(value.items())},
            type='object',
        )
    return {}


def _types(schema: Dict) -> List[str]:
    found = schema.get('type')
    if found is None:
        return []
    return [found] if isinstance(found, str) else list(found)


def merge(a: Dict, b: Dict) -> Dict:
    """The schema that admits both: types joined, properties and items merged."""
    if not a:
        return dict(b)
    if not b:
        return dict(a)
    types = sorted(set(_types(a)) | set(_types(b)))
    merged: Dict = {}
    if 'object' in types:
        properties = dict(a.get('properties') or {})
        for key, schema in (b.get('properties') or {}).items():
            properties[key] = merge(properties.get(key) or {}, schema)
        merged['properties'] = dict(sorted(properties.items()))
    if 'array' in types:
        merged['items'] = merge(a.get('items') or {}, b.get('items') or {})
    if types:
        merged['type'] = types[0] if len(types) == 1 else types
    return merged


class Recorder:
    """Schemas inferred per resource from the responses it was given."""

    def __init__(self):
        self.schemas: Dict[str, Dict] = {}
        self.calls: List[Tuple[str, str, str]] = []

    def take(self, method: str, path: str, resource: str, payload: Any) -> Optional[str]:
        """Records a response; returns the problem when it holds no record of the resource."""
        if not isinstance(payload, dict):
            return 'not JSON'
        if payload.get('error'):
            error = payload['error']
            # A 500's message is the traceback; its exception says what failed.
            reason = error.get('exception') if error.get('code') == 500 else None
            reason = str(reason or error.get('message') or error.get('type')).strip()
            return f"error {error.get('code')}: {reason.splitlines()[0] if reason else ''}"
        singular = inflection.singularize(resource)
        records = payload.get(resource)
        if records is None and singular in payload:
            records = [payload[singular]]
        if not isinstance(records, list):
            return f'no {resource} or {singular} in the response'
        name = _schema_name(resource)
        for record in records:
            self.schemas[name] = merge(self.schemas.get(name) or {}, schema_of(record))
        self.calls.append((method, path, f'{len(records)} records'))
        return None


def apply(spec: Dict, recorder: Recorder, unreached: Dict[str, str]) -> Dict:
    """The static spec with the recorded schemas; unreached resources listed with why."""
    schemas = spec['components']['schemas']
    for name, recorded in recorder.schemas.items():
        static = schemas.get(name) or dict(properties={}, type='object')
        recorded = dict(recorded)
        # Presenter attributes the responses did not hold stay, with no type.
        properties = {key: {} for key in static.get('properties') or {}}
        properties.update(recorded.get('properties') or {})
        recorded['properties'] = dict(sorted(properties.items()))
        schemas[name] = recorded
    spec['x-mage-unrecorded'] = dict(sorted(unreached.items()))
    return spec


def dumps(spec: Dict) -> str:
    return json.dumps(spec, indent=1, sort_keys=True) + '\n'


Fetch = Callable[[str, str, Optional[Dict]], Any]


LOADER = '''@data_loader
def load_numbers(*args, **kwargs):
    return [dict(id=1, value=2.5)]
'''
TRANSFORMER = '''@transformer
def double(rows, *args, **kwargs):
    return [dict(row, value=row['value'] * 2) for row in rows]
'''
PIPELINE = 'contract_pipeline'
# Resources whose list calls a cloud, a cluster or an external program the scratch project
# has none of; they keep their presenter attributes.
NOT_CALLED = {
    'clusters': 'lists EMR, Kubernetes or ECS clusters',
    'compute_clusters': 'calls the compute service of the project (cloud)',
    'compute_connections': 'calls the compute service of the project (cloud)',
    'compute_services': 'calls the compute service of the project (cloud)',
    'instances': 'lists cloud instances',
    'kernels': 'starts a Jupyter kernel',
    'spark_applications': 'calls a Spark UI',
    'spark_environments': 'calls a Spark UI',
    'spark_executors': 'calls a Spark UI',
    'spark_jobs': 'calls a Spark UI',
    'spark_sqls': 'calls a Spark UI',
    'spark_stage_attempt_task_summaries': 'calls a Spark UI',
    'spark_stage_attempt_tasks': 'calls a Spark UI',
    'spark_stage_attempts': 'calls a Spark UI',
    'spark_stages': 'calls a Spark UI',
    'spark_threads': 'calls a Spark UI',
    'workspaces': 'lists cloud workspaces',
}


def _key(record: Dict) -> Optional[str]:
    for field in ('id', 'uuid', 'name'):
        value = record.get(field) if isinstance(record, dict) else None
        if value not in (None, ''):
            return str(value)
    return None


def record(fetch: Fetch, spec: Dict, make_run: Callable[[int], int]) -> Tuple[Recorder, Dict]:
    """
    Seeds the project through the API and records the responses of every list and detail
    it can reach. fetch(method, path, body) returns the parsed JSON; make_run(schedule_id)
    adds a pipeline run without running it and returns its id.
    """
    from urllib.parse import quote

    recorder = Recorder()
    problems: Dict[str, str] = {}

    def call(method: str, path: str, resource: str, body: Optional[Dict] = None) -> Any:
        try:
            payload = fetch(method, path, body)
        except Exception as error:
            problems.setdefault(resource, f'{method} {path}: {error}')
            return None
        problem = recorder.take(method, path, resource, payload)
        if problem:
            problems.setdefault(resource, f'{method} {path}: {problem}')
        return payload

    call('post', '/api/pipelines', 'pipelines',
         dict(pipeline=dict(name=PIPELINE, type='python')))
    for name, block_type, content, upstream in (
        ('load_numbers', 'data_loader', LOADER, []),
        ('double', 'transformer', TRANSFORMER, ['load_numbers']),
    ):
        call('post', f'/api/pipelines/{PIPELINE}/blocks', 'blocks', dict(block=dict(
            content=content,
            language='python',
            name=name,
            type=block_type,
            upstream_blocks=upstream,
        )))
    created = call('post', f'/api/pipelines/{PIPELINE}/pipeline_schedules',
                   'pipeline_schedules', dict(pipeline_schedule=dict(
                       name='contract_trigger',
                       schedule_interval='@once',
                       schedule_type='time',
                       status='inactive',
                   ))) or {}
    schedule_id = (created.get('pipeline_schedule') or {}).get('id')
    call('put', f'/api/pipelines/{PIPELINE}', 'pipelines',
         dict(pipeline=dict(tags=['contract'])))
    call('post', f'/api/pipelines/{PIPELINE}/backfills', 'backfills', dict(backfill=dict(
        end_datetime='2026-01-02T00:00:00',
        interval_type='day',
        interval_units=1,
        name='contract_backfill',
        start_datetime='2026-01-01T00:00:00',
    )))
    call('post', '/api/secrets', 'secrets', dict(secret=dict(name='contract_secret',
                                                             value='not-a-secret')))
    call('post', '/api/roles', 'roles', dict(role=dict(name='contract_role')))
    call('get', f"/api/file_contents/{quote(f'pipelines/{PIPELINE}/metadata.yaml', safe='')}",
         'file_contents')
    parents = dict(pipelines=PIPELINE)
    if schedule_id is not None:
        parents['pipeline_schedules'] = str(schedule_id)
        parents['pipeline_runs'] = str(make_run(schedule_id))

    paths = spec['paths']
    for resource in spec['x-mage-resources']:
        if resource in NOT_CALLED or 'get' not in paths.get(f'/api/{resource}', {}):
            continue
        listed = call('get', f'/api/{resource}', resource) or {}
        records = listed.get(resource) if isinstance(listed, dict) else None
        key = _key(records[0]) if isinstance(records, list) and records else None
        if key and 'get' in paths.get(f'/api/{resource}/{{id}}', {}):
            call('get', f'/api/{resource}/{quote(key, safe="")}', resource)
    for path, item in paths.items():
        parts = path.split('/')
        if len(parts) != 5 or parts[3] != '{parent_id}' or 'get' not in item:
            continue
        parent, resource = parts[2], parts[4]
        if parent not in parents or resource in NOT_CALLED:
            continue
        nested = f'/api/{parent}/{quote(parents[parent], safe="")}/{resource}'
        listed = call('get', nested, resource) or {}
        records = listed.get(resource) if isinstance(listed, dict) else None
        key = _key(records[0]) if isinstance(records, list) and records else None
        if key and 'get' in paths.get(f'{path}/{{id}}', {}):
            call('get', f'{nested}/{quote(key, safe="")}', resource)
    if parents.get('pipeline_runs'):
        call('get', f"/api/pipeline_runs/{parents['pipeline_runs']}", 'pipeline_runs')

    unreached = {}
    for resource, about in spec['x-mage-resources'].items():
        if about['schema'] in recorder.schemas:
            continue
        if resource in NOT_CALLED:
            unreached[resource] = f'not called: {NOT_CALLED[resource]}'
        elif resource in problems:
            unreached[resource] = problems[resource]
        else:
            unreached[resource] = 'no list or detail reached'
    return recorder, unreached
