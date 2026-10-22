"""
Airflow Assets for MageML pipelines (Airflow 3, data-aware scheduling):

    from mageml_airflow.assets import mageml_asset

    scores = mageml_asset('score_orders', block='summary')
    MageMLServiceRunOperator(task_id='score', pipeline='score_orders', outlets=[scores])

    with DAG('report', schedule=[scores]):   # runs when a score_orders run completed
        ...

A task with the asset in its outlets records an asset event when it succeeds, so DAGs
scheduled on it run after each MageML run Airflow started.
"""
from typing import Optional

try:  # Airflow 3
    from airflow.sdk import Asset
except ImportError:  # Airflow 2.10
    from airflow.datasets import Dataset as Asset

SCHEME = 'mageml'


def mageml_asset(pipeline: str, block: Optional[str] = None,
                 conn_id: str = 'mageml_default') -> 'Asset':
    """
    The asset of a pipeline (or of one block's output) of the MageML service or server
    behind `conn_id`: mageml://<conn_id>/<pipeline>[/<block>].
    """
    path = f'{pipeline}/{block}' if block else pipeline
    uri = f'{SCHEME}://{conn_id}/{path}'
    try:
        return Asset(uri=uri, name=f'mageml {conn_id} {path}')
    except TypeError:  # Airflow 2.10's Dataset takes the URI only
        return Asset(uri)
