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


# The io_config.yaml functions of each cloud's secret store, and the providers of the
# service's MAGE_SERVICE_SECRETS that fetch the same secrets.
CLOUD_SECRET_FUNCTIONS = {
    'aws_secret_var': 'aws',
    'azure_secret_var': 'azure',
    'gcp_secret_var': 'gcp',
    'ibm_secret_var': 'ibm',
}


def cloud_secret_variable_name(provider: str, secret_id: str, field: str = None) -> str:
    """The environment variable a service fetches a cloud secret function's value into."""
    slug = re.sub(r'[^A-Za-z0-9]+', '_', secret_id + (f'_{field}' if field else ''))
    return f'{provider.upper()}_SECRET_' + slug.strip('_').upper()


def cloud_secret_source(provider: str, secret_id: str, field: str = None) -> str:
    """The service's reference to the same secret, as MAGE_SERVICE_SECRETS writes it."""
    return f'{provider}:{secret_id}' + (f'#{field}' if field else '')


def service_cloud_secret(provider: str, secret_id: str, field: str = None) -> str:
    """A cloud secret function's value inside a service, fetched at start-up."""
    name = cloud_secret_variable_name(provider, secret_id, field)
    value = os.environ.get(name)
    if value is None:
        raise KeyError(
            f'{name} is not set: the service fetches it at start-up from {provider} '
            'secrets; check its credentials and the start-up log.'
        )
    return value


def ibm_secret_variable_name(secret_id: str, field: str = None) -> str:
    return cloud_secret_variable_name('ibm', secret_id, field)


def ibm_secret_source(secret_id: str, field: str = None) -> str:
    return cloud_secret_source('ibm', secret_id, field)
