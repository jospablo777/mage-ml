import json
from typing import Dict, Union
from unittest.mock import MagicMock, call, patch

from mage_ai.api.operations.constants import OperationType
from mage_ai.api.views import execute_operation
from mage_ai.orchestration.db.models.oauth import (
    Oauth2AccessToken,
    Oauth2Application,
    User,
)
from mage_ai.shared.hash import merge_dict
from mage_ai.tests.base_test import AsyncDBTestCase


class CustomRequest:
    def __init__(
        self,
        body: str = None,
        body_arguments: Dict = None,
        current_user: User = None,
        error: Dict = None,
        files: Dict = None,
        headers: Dict = None,
        method: str = None,
        oauth_client: Oauth2Application = None,
        oauth_token: Oauth2AccessToken = None,
        query_arguments: Dict = None,
    ):
        self.body = body or ''
        self.body_arguments = body_arguments or {}
        self.current_user = current_user
        self.error = error
        self.files = files or {}
        self.headers = headers or {}
        self.method = method
        self.oauth_client = oauth_client
        self.oauth_token = oauth_token
        self.query_arguments = query_arguments or {}


def build_handler(request: CustomRequest) -> MagicMock:
    handler = MagicMock()
    handler.request = request

    return handler


async def build_case(
    request: CustomRequest,
    response: Dict,
    handler_write_called_with: Union[Dict, str],
    operation_called_with: Dict,
    action: OperationType,
    resource: str,
    resource_pk: Union[None, int, str] = None,
    child: str = None,
    child_pk: Union[None, int, str] = None,
):
    mock_operation_init = MagicMock()

    class CustomBaseOperation:
        async def execute(cls):
            return response

    def _test(*args, **kwargs):
        mock_operation_init(*args, **kwargs)

        return CustomBaseOperation()

    with patch('mage_ai.api.views.BaseOperation', _test):
        mock_handler = build_handler(request)

        await execute_operation(
            mock_handler,
            resource=resource,
            pk=resource_pk,
            child=child,
            child_pk=child_pk,
        )

        called_with = merge_dict(dict(
            action=action,
            files=request.files,
            headers=request.headers,
            oauth_client=request.oauth_client,
            oauth_token=request.oauth_token,
            pk=child_pk or resource_pk,
            remote_ip=None,
            resource=child or resource,
            resource_parent=resource if child else None,
            resource_parent_id=resource_pk if child else None,
            user=request.current_user,
        ), operation_called_with)

        mock_operation_init.assert_called_once_with(**called_with)

        mock_handler.assert_has_calls([
            call.write(handler_write_called_with)
        ])


class ApiViewsTest(AsyncDBTestCase):
    pass


for config in [
    dict(
        action=OperationType.LIST,
        operation_called_with=dict(
            meta=dict(_limit=1),
            options=dict(fire=1, _limit=1),
            query=dict(fire=1),
        ),
        request=CustomRequest(
            method='GET',
            query_arguments=dict(fire=1, _limit=1),
        ),
    ),
    dict(
        action=OperationType.LIST,
        operation_called_with=dict(
            meta=dict(_limit=1),
            options=dict(fire=1, _limit=1),
            query=dict(fire=1),
        ),
        request=CustomRequest(
            method='GET',
            query_arguments=dict(fire=1, _limit=1),
        ),
        resource_pk=1,
        child='spells',
    ),
    dict(
        action=OperationType.CREATE,
        operation_called_with=dict(
            options=dict(fire=1),
            payload=dict(fire=1),
        ),
        request=CustomRequest(
            method='POST',
            body_arguments=dict(fire=1),
        ),
    ),
    dict(
        action=OperationType.CREATE,
        operation_called_with=dict(
            options=dict(fire=1),
            payload=dict(fire=1),
        ),
        request=CustomRequest(
            method='POST',
            body_arguments=dict(fire=1),
        ),
        resource_pk=1,
        child='spells',
    ),
    dict(
        action=OperationType.DETAIL,
        operation_called_with=dict(
            meta=dict(_limit=1),
            options=dict(fire=1, _limit=1),
            query=dict(fire=1),
        ),
        request=CustomRequest(
            method='GET',
            query_arguments=dict(fire=1, _limit=1),
        ),
        resource_pk=1,
    ),
    dict(
        action=OperationType.DETAIL,
        operation_called_with=dict(
            meta=dict(_limit=1),
            options=dict(fire=1, _limit=1),
            query=dict(fire=1),
        ),
        request=CustomRequest(
            method='GET',
            query_arguments=dict(fire=1, _limit=1),
        ),
        resource_pk=1,
        child='spells',
        child_pk=2,
    ),
    dict(
        action=OperationType.UPDATE,
        operation_called_with=dict(
            meta=dict(_limit=1),
            options=dict(wind=3),
            payload=dict(wind=3),
            query=dict(fire=1),
        ),
        request=CustomRequest(
            method='PUT',
            query_arguments=dict(fire=1, _limit=1),
            body_arguments=dict(wind=3),
        ),
        resource_pk=1,
    ),
    dict(
        action=OperationType.UPDATE,
        operation_called_with=dict(
            meta=dict(_limit=1),
            options=dict(wind=3),
            payload=dict(wind=3),
            query=dict(fire=1),
        ),
        request=CustomRequest(
            method='PUT',
            query_arguments=dict(fire=1, _limit=1),
            body_arguments=dict(wind=3),
        ),
        resource_pk=1,
        child='spells',
        child_pk=2,
    ),
    dict(
        action=OperationType.DELETE,
        operation_called_with=dict(
            meta=dict(_limit=1),
            options=dict(wind=3),
            payload=dict(wind=3),
            query=dict(fire=1),
        ),
        request=CustomRequest(
            method='DELETE',
            query_arguments=dict(fire=1, _limit=1),
            body_arguments=dict(wind=3),
        ),
        resource_pk=1,
    ),
    dict(
        action=OperationType.DELETE,
        operation_called_with=dict(
            meta=dict(_limit=1),
            options=dict(wind=3),
            payload=dict(wind=3),
            query=dict(fire=1),
        ),
        request=CustomRequest(
            method='DELETE',
            query_arguments=dict(fire=1, _limit=1),
            body_arguments=dict(wind=3),
        ),
        resource_pk=1,
        child='spells',
        child_pk=2,
    ),
]:
    action = config['action']
    request = config['request']

    response = config.get('response') or dict(error=None)
    handler_write_called_with = \
        config.get('handler_write_called_with') or json.dumps(dict(error=None))
    operation_called_with = merge_dict(dict(
        query={},
        payload={},
        options={},
        meta={},
    ), config.get('operation_called_with') or {})

    child = config.get('child')
    child_pk = config.get('child_pk')
    resource_pk = config.get('resource_pk')

    def _buid_test_execute_operation(
        resource_pk: int = None,
        child: str = None,
        child_pk: int = None,
        action=action,
        handler_write_called_with=handler_write_called_with,
        operation_called_with=operation_called_with,
        response=response,
        request=request,
    ):
        async def _test_execute_operation(
            self,
            action=action,
            child=child,
            child_pk=child_pk,
            handler_write_called_with=handler_write_called_with,
            operation_called_with=operation_called_with,
            resource_pk=resource_pk,
            response=response,
            request=request,
        ):
            await build_case(
                request,
                response,
                handler_write_called_with,
                operation_called_with,
                action,
                resource='powers',
                resource_pk=resource_pk,
                child=child,
                child_pk=child_pk,
            )
        return _test_execute_operation

    def _buid_test_execute_operation_error(
        resource_pk: int = None,
        child: str = None,
        child_pk: int = None,
        action=action,
        handler_write_called_with=handler_write_called_with,
        operation_called_with=operation_called_with,
        request=request,
    ):
        async def _test_execute_operation_error(
            self,
            action=action,
            child=child,
            child_pk=child_pk,
            operation_called_with=operation_called_with,
            resource_pk=resource_pk,
            request=request,
        ):
            await build_case(
                request,
                dict(error='fire'),
                dict(
                    error='fire',
                    status=200,
                ),
                operation_called_with,
                action,
                resource='powers',
                resource_pk=resource_pk,
                child=child,
                child_pk=child_pk,
            )
        return _test_execute_operation_error

    method_name = '_'.join(list(filter(
        lambda x: x,
        [
            f'test_execute_operation_{action}',
            str(resource_pk) if resource_pk else resource_pk,
            child,
            str(child_pk) if child_pk else child_pk,
        ],
    )))
    setattr(
        ApiViewsTest,
        method_name,
        _buid_test_execute_operation(
            child=child,
            child_pk=child_pk,
            resource_pk=resource_pk,
        ),
    )

    setattr(
        ApiViewsTest,
        f'{method_name}_error',
        _buid_test_execute_operation_error(
            child=child,
            child_pk=child_pk,
            resource_pk=resource_pk,
        ),
    )


class RequestTransactionTest(AsyncDBTestCase):
    """
    I-466: a request that only read left its transaction open on the server thread, and
    SQLite kept serving that thread the snapshot it began with: the runs page showed a run
    as running two minutes after it completed. Each request now ends its transaction.
    """

    async def test_a_read_does_not_leave_the_thread_on_an_old_snapshot(self):
        from mage_ai.orchestration.db import db_connection
        from mage_ai.orchestration.db.models.schedules import PipelineRun
        from mage_ai.tests.factory import (
            create_pipeline_run_with_schedule,
            create_pipeline_with_blocks,
        )

        pipeline = create_pipeline_with_blocks(self.faker.unique.name(), self.repo_path)
        run = create_pipeline_run_with_schedule(
            pipeline_uuid=pipeline.uuid, status=PipelineRun.PipelineRunStatus.RUNNING,
        )
        run_id = run.id
        db_connection.session.commit()

        class Reading:
            async def execute(self):
                status = PipelineRun.get_by_id(run_id).status
                return dict(pipeline_run=dict(status=str(status)))

        with patch('mage_ai.api.views.BaseOperation', lambda *args, **kwargs: Reading()):
            await execute_operation(build_handler(CustomRequest(method='GET')),
                                    resource='pipeline_runs', pk=run_id)
        self.assertFalse(db_connection.session().in_transaction())

        # Another process (the scheduler) completes the run.
        engine = db_connection.session().get_bind()
        with engine.begin() as other:
            other.exec_driver_sql(
                "UPDATE pipeline_run SET status = 'COMPLETED' WHERE id = ?", (run_id,))
        self.assertEqual(PipelineRun.get_by_id(run_id).status,
                         PipelineRun.PipelineRunStatus.COMPLETED)

    async def test_a_failed_request_rolls_back_what_it_did_not_commit(self):
        from mage_ai.orchestration.db import db_connection

        class Failing:
            async def execute(self):
                db_connection.session.execute(text_sql('SELECT 1'))
                raise RuntimeError('broken')

        with patch('mage_ai.api.views.BaseOperation', lambda *args, **kwargs: Failing()):
            with self.assertRaises(RuntimeError):
                await execute_operation(build_handler(CustomRequest(method='GET')),
                                        resource='pipeline_runs', pk=1)
        self.assertFalse(db_connection.session().in_transaction())


def text_sql(statement):
    from sqlalchemy import text

    return text(statement)
