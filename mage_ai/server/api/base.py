import json
import re
import traceback
import urllib.parse

import dateutil.parser
import simplejson
import tornado.web

from mage_ai.api.errors import ApiError
from mage_ai.api.middleware import OAuthMiddleware
from mage_ai.server.api.constants import PATH_TRAVERSAL_PATTERN
from mage_ai.server.logger import Logger
from mage_ai.settings import REQUIRE_USER_AUTHENTICATION
from mage_ai.shared.parsers import encode_complex
from mage_ai.shared.strings import camel_to_snake_case

logger = Logger().new_server_logger(__name__)

META_KEY_LIMIT = '_limit'
META_KEY_OFFSET = '_offset'


class BaseHandler(tornado.web.RequestHandler):
    datetime_keys = []
    model_class = None

    def check_origin(self, origin):
        return True

    def get_bool_argument(self, name, default_value=None):
        value = self.get_argument(name, default_value)
        if type(value) is not str:
            return value
        return value.lower() in ('yes', 'true', 't', '1')

    def limit(self, results):
        limit = self.get_argument(META_KEY_LIMIT, None)
        offset = self.get_argument(META_KEY_OFFSET, None)
        if limit is not None:
            results = results.limit(limit)
        if offset is not None:
            results = results.offset(offset)
        return results

    def options(self, **kwargs):
        self.set_status(204)
        self.finish()

    def set_default_headers(self):
        methods = 'DELETE, GET, PATCH, POST, PUT, OPTIONS'
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Allow-Methods', methods)
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Content-Type', 'application/json')

    def write(self, chunk):
        if type(chunk) is dict:
            chunk = simplejson.dumps(
                chunk,
                default=encode_complex,
                ignore_nan=True,
            )
        super().write(chunk)

    def write_error(self, status_code, **kwargs):
        if status_code == 500:
            self.set_status(200)
            exception = kwargs['exc_info'][1]
            if isinstance(exception, ApiError):
                # A refused request: its code and reason, without the server's traceback.
                error = dict(exception.to_dict(), exception=exception.message)
                self.write(dict(error=error, url_parameters=self.path_kwargs))
                return
            # The exception's traceback, with its notes. format_stack() here gave the stack
            # of this error handler, which said nothing about the failure.
            message = ''.join(traceback.format_exception(exception))
            if REQUIRE_USER_AUTHENTICATION and not getattr(self.request, 'current_user', None):
                # Not signed in (an API trigger's token, or no sign-in yet): the exception's
                # own message, and the traceback in the log. The traceback went to the client,
                # every frame's path and code included.
                logger.error(f'{self.request.method} {self.request.path}:\n{message}')
                message = 'The server could not answer this request; its log has the details.'

            self.write(
                dict(
                    error=dict(
                        code=status_code,
                        errors=[],
                        exception=str(exception),
                        message=message,
                    ),
                    url_parameters=self.path_kwargs,
                )
            )

    def write_model(self, model, **kwargs):
        key = camel_to_snake_case(self.model_class.__name__)
        self.write({key: model.to_dict(**kwargs)})

    def write_models(self, models):
        key = camel_to_snake_case(self.model_class.__name__) + 's'
        self.write({key: [m.to_dict() for m in models]})

    def get_payload(self):
        key = ''
        if self.model_class:
            key = camel_to_snake_case(self.model_class.__name__)

        payload = {}
        body = self.request.body
        if body:
            payload = json.loads(self.request.body)
            if key != '':
                payload = payload.get(key, {})
            for key in self.datetime_keys:
                if payload.get(key) is not None:
                    payload[key] = dateutil.parser.parse(payload[key])
        return payload


class BaseApiHandler(BaseHandler, OAuthMiddleware):
    def initialize(self, **kwargs) -> None:
        super().initialize(**kwargs)
        self.is_health_check = kwargs.get('is_health_check', False)

    def is_safe_path(self, user_input):
        """
        Check if the user input is safe and doesn't contain path traversal sequences.
        """
        return re.match(PATH_TRAVERSAL_PATTERN, user_input) is not None

    def prepare(self):
        from mage_ai.server.server import latest_user_activity

        if not self.is_health_check:
            latest_user_activity.update_latest_activity()

        # Validate the request path by decoding from bytes to string if necessary
        decoded_path = self.request.path
        if isinstance(decoded_path, bytes):
            decoded_path = decoded_path.decode('utf-8')  # Decode only if it's a bytes object

        decoded_path = urllib.parse.unquote(decoded_path)  # Decode URL-encoded characters
        if not self.is_safe_path(decoded_path):
            self.set_status(400)  # Bad Request
            self.write("Error: Invalid path (path traversal detected)")
            self.finish()
            return

        # Validate query parameters (if any)
        for key, value in self.request.arguments.items():
            # Decode each key and value from bytes to string if necessary
            decoded_key = key
            if isinstance(decoded_key, bytes):
                decoded_key = decoded_key.decode('utf-8')

            decoded_value = value[0]
            if isinstance(decoded_value, bytes):
                decoded_value = decoded_value.decode('utf-8')

            decoded_value = urllib.parse.unquote(decoded_value)  # Decode URL-encoded characters
            if not self.is_safe_path(decoded_value):
                self.set_status(400)
                self.write(
                    f"Error: Invalid parameter value for '{decoded_key}' (path traversal detected)"
                )
                self.finish()
                return
        super().prepare()


class SignedInMixin(OAuthMiddleware):
    """
    For handlers outside the API resources: with REQUIRE_USER_AUTHENTICATION, only a
    signed-in user gets an answer, as from the resources. These handlers derived from
    BaseHandler alone, so anyone who could reach the server could trigger pipelines
    through /api/events, change event matchers and read output statistics.
    """

    def prepare(self):
        from mage_ai import settings

        super().prepare()
        if settings.REQUIRE_USER_AUTHENTICATION and (
            getattr(self.request, 'error', None) or not getattr(self.request, 'current_user', None)
        ):
            self.set_status(401)
            self.set_header('Content-Type', 'application/json')
            self.finish(json.dumps(dict(error=dict(code=401, message='Sign in first.'))))


def may_view_pipeline(user, pipeline_uuid: str, required: bool) -> bool:
    """
    has_at_least_viewer_role on the pipeline, for handlers outside the API resources: being
    signed in let a user with no role, or kept out of the pipeline, read its outputs.
    `required` is the caller's REQUIRE_USER_AUTHENTICATION.
    """
    if not required:
        return True
    from mage_ai.api.utils import has_at_least_viewer_role
    from mage_ai.orchestration.constants import Entity

    return user is not None and has_at_least_viewer_role(user, Entity.PIPELINE, pipeline_uuid)


class EditorMixin(SignedInMixin):
    """
    SignedInMixin, and changes only by an editor of the project: any signed-in user, a
    viewer too, could trigger event pipelines through /api/events and create, change or
    delete event matchers.
    """

    # Reads too, where what is read is code output.
    editors_only_read = False

    def prepare(self):
        from mage_ai import settings

        super().prepare()
        if self._finished or not settings.REQUIRE_USER_AUTHENTICATION:
            return
        if self.request.method in ('GET', 'HEAD', 'OPTIONS') and not self.editors_only_read:
            return
        from mage_ai.api.utils import has_at_least_editor_role
        from mage_ai.data_preparation.repo_manager import get_project_uuid
        from mage_ai.orchestration.constants import Entity

        if not has_at_least_editor_role(
            self.request.current_user, Entity.PROJECT, get_project_uuid(),
        ):
            self.set_status(403)
            self.set_header('Content-Type', 'application/json')
            self.finish(json.dumps(dict(error=dict(
                code=403, message='Only editors make changes here.',
            ))))


class BaseDetailHandler(BaseHandler):
    def get(self, model_id, **kwargs):
        model = self.model_class.get_by_id(int(model_id))
        self.write_model(model, **kwargs)

    def put(self, model_id, payload=None):
        model = self.model_class.get_by_id(int(model_id))
        payload = payload or self.get_payload()
        model.update(**payload)
        self.write_model(model)

    def delete(self, model_id):
        model = self.model_class.get_by_id(int(model_id))
        model.delete()
        self.write_model(model)
