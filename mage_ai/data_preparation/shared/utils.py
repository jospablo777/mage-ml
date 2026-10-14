import os
from typing import Callable, Dict

from mage_ai.shared.hash import get_json_value, merge_dict


def _mage_secret_var(name, *args, **kwargs):
    # A pipeline service has no metadata database: its secrets come from the environment,
    # as MAGE_SECRET_<NAME>, which the service fills from its secret files.
    if os.environ.get('MAGE_SERVICE'):
        from mage_ai.pipeline_services.secrets import service_secret

        return service_secret(name)
    # Secrets need the metadata database and cryptography; they load when a template
    # reads a secret, not for every io_config.yaml.
    from mage_ai.data_preparation.shared.secrets import get_secret_value

    return get_secret_value(name, *args, **kwargs)


def get_template_vars(include_python_libraries: Dict = None) -> Dict[str, Callable]:
    no_db_kwargs = get_template_vars_no_db(include_python_libraries=include_python_libraries)

    kwargs = dict(mage_secret_var=_mage_secret_var)

    return merge_dict(no_db_kwargs, kwargs)


def get_template_vars_no_db(include_python_libraries: Dict = None) -> Dict[str, Callable]:
    kwargs = dict(
        env_var=os.getenv,
        json_value=get_json_value,
    )

    if include_python_libraries:
        kwargs.update(include_python_libraries)

    kwargs.update(_cloud_secret_functions())
    return kwargs


def _cloud_secret_functions() -> Dict[str, Callable]:
    """
    The cloud secret stores' io_config.yaml functions. Each loads its client when a template
    calls it; inside an exported service, which fetched the secrets at start-up, each reads
    its environment variable.
    """
    def in_service(provider, secret_id, field=None):
        from mage_ai.pipeline_services.secrets import service_cloud_secret

        return service_cloud_secret(provider, secret_id, field)

    def aws_secret_var(secret_id, cached=True):
        if os.environ.get('MAGE_SERVICE'):
            return in_service('aws', secret_id)
        from mage_ai.services.aws.secrets_manager.secrets_manager import get_secret

        return get_secret(secret_id, cached)

    def azure_secret_var(secret_name):
        if os.environ.get('MAGE_SERVICE'):
            return in_service('azure', secret_name)
        from mage_ai.services.azure.key_vault.key_vault import get_secret

        return get_secret(secret_name)

    def gcp_secret_var(secret, field=None):
        if os.environ.get('MAGE_SERVICE'):
            return in_service('gcp', secret, field)
        from mage_ai.services.gcp.secret_manager import get_secret

        return get_secret(secret, field)

    def ibm_secret_var(secret_id, field=None):
        from mage_ai.services.ibm.secrets_manager import get_secret

        return get_secret(secret_id, field)

    return dict(aws_secret_var=aws_secret_var, azure_secret_var=azure_secret_var,
                gcp_secret_var=gcp_secret_var, ibm_secret_var=ibm_secret_var)
