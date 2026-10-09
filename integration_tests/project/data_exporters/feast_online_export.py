import requests

from mage_ai.shared.pandas_utils import missing_as_none

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame, **kwargs) -> None:
    # JSON needs None for missing values and plain Python values.
    columns = missing_as_none(frame)
    response = requests.post(
        # /push/typed keeps integers above 2**53 exact next to NULL values; Feast's /push
        # rounds them. See integration_tests/services/feast/typed_push.py.
        f"{kwargs['feast_url']}/push/typed",
        json=dict(
            push_source_name='driver_stats_push',
            to='online',
            df={column: columns[column].tolist() for column in columns.columns},
        ),
        timeout=60,
    )
    response.raise_for_status()
