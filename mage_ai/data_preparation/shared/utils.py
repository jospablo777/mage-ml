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

    try:
        from mage_ai.services.aws.secrets_manager.secrets_manager import get_secret
        kwargs['aws_secret_var'] = get_secret
    except Exception:
        pass

    try:
        from mage_ai.services.azure.key_vault.key_vault import get_secret
        kwargs['azure_secret_var'] = get_secret
    except Exception:
        pass

    return kwargs
