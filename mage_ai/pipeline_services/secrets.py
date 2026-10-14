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


def ibm_secret_variable_name(secret_id: str, field: str = None) -> str:
    """The environment variable a service fetches `ibm_secret_var(secret_id, field)` into."""
    slug = re.sub(r'[^A-Za-z0-9]+', '_', secret_id + (f'_{field}' if field else ''))
    return 'IBM_SECRET_' + slug.strip('_').upper()


def ibm_secret_source(secret_id: str, field: str = None) -> str:
    """The service's reference to the same secret, as MAGE_SERVICE_SECRETS writes it."""
    return f'ibm:{secret_id}' + (f'#{field}' if field else '')
