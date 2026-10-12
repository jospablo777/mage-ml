from types import MappingProxyType
from typing import Dict, Optional

from mage_ai.errors.constants import ErrorCode
from mage_ai.errors.models import ErrorDetails


class ApiError(Exception):
    # Read-only: an error is built from a copy, dict(ApiError.RESOURCE_NOT_FOUND). Updating
    # the shared dict changed the message of every later error of that kind in the process.
    UNAUTHORIZED_ACCESS = MappingProxyType(dict(
        code=ErrorCode.CODE_403.value,
        message='Unauthorized access.',
        type='unauthorized_access',
    ))

    RESOURCE_ERROR = MappingProxyType(dict(
        code=ErrorCode.CODE_400.value,
        message='API resource error.',
        type='api_resource_error',
    ))
    RESOURCE_INVALID = MappingProxyType(dict(
        code=ErrorCode.CODE_400.value,
        message='Record is invalid.',
        type='record_invalid',
    ))
    RESOURCE_NOT_FOUND = MappingProxyType(dict(
        code=ErrorCode.CODE_404.value,
        message='Record not found.',
        type='record_not_found',
    ))

    INVALID_API_KEY = MappingProxyType(dict(
        code=ErrorCode.CODE_401.value,
        message='Invalid API key.',
        type='invalid_api_key',
    ))
    EXPIRED_OAUTH_TOKEN = MappingProxyType(dict(
        code=ErrorCode.CODE_401.value,
        message='Expired OAuth token.',
        type='expired_oauth_token',
    ))
    INVALID_OAUTH_TOKEN = MappingProxyType(dict(
        code=ErrorCode.CODE_401.value,
        message='Invalid OAuth token.',
        type='invalid_oauth_token',
    ))

    def __init__(self, opts: Optional[Dict] = None):
        if opts:
            self.code = opts.get('code')
            self.errors = opts.get('errors')
            self.message = opts.get('message')
            self.type = opts.get('type')

    def to_dict(self, **kwargs) -> Dict:
        errors = self.errors
        if not isinstance(errors, list):
            errors = str(errors).split('\n')

        return ErrorDetails.load(
            code=self.code,
            errors=errors,
            message=self.message,
            type=self.type,
        ).to_dict()
