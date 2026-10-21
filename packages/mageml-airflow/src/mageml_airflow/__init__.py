"""Run pipelines of MageML services from Airflow (MageMLServiceRunOperator)."""

__version__ = '0.1.0'


def get_provider_info():
    """What Airflow lists for this provider: the MageML service connection type."""
    return {
        'package-name': 'mageml-airflow',
        'name': 'MageML',
        'description': 'Runs pipelines of MageML services.',
        'versions': [__version__],
        'connection-types': [{
            'connection-type': 'mageml_service',
            'hook-class-name': 'mageml_airflow.hooks.MageMLServiceHook',
        }],
    }
