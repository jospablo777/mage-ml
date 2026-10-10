"""Mage secrets inside a pipeline service, where there is no metadata database."""
import os
import re


def secret_variable_name(name: str) -> str:
    """The environment variable that holds the Mage secret `name` in a service."""
    return 'MAGE_SECRET_' + re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_').upper()


def service_secret(name: str) -> str:
    variable = secret_variable_name(name)
    value = os.environ.get(variable)
    if value is None:
        raise KeyError(
            f'The secret {name!r} is not set. Pipeline services read Mage secrets from the '
            f'environment: set {variable}, {variable}_FILE or a file named {variable} in '
            'MAGE_SERVICE_SECRETS_DIR.'
        )
    return value
