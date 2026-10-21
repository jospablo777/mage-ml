"""
Run pipelines from Airflow: of MageML services (MageMLServiceRunOperator) and of MageML
servers (MageMLPipelineOperator).
"""

__version__ = '0.1.0'


def get_provider_info():
    """What Airflow lists for this provider: the MageML service connection type."""
    return {
        'package-name': 'mageml-airflow',
        'name': 'MageML',
        'description': 'Runs pipelines of MageML services and servers.',
        'versions': [__version__],
        'connection-types': [
            {
                'connection-type': 'mageml_service',
                'hook-class-name': 'mageml_airflow.hooks.MageMLServiceHook',
            },
            {
                'connection-type': 'mageml_server',
                'hook-class-name': 'mageml_airflow.hooks.MageMLServerHook',
            },
        ],
    }
