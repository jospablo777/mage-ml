from os import path

from mage_ai.io.amplitude import Amplitude
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_amplitude(data, **kwargs) -> None:
    """
    Sends rows to Amplitude as user properties (or as events, below). Set AMPLITUDE_API_KEY
    and AMPLITUDE_REGION (US or EU) in io_config.yaml: Amplitude > Settings > Projects >
    your project > General. Sending needs no secret key.

    Each row has user_id (or device_id); every other column is a property to set. Rows
    are sent 2,000 a request, and each event's insert_id keeps it from being sent twice.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    amplitude = Amplitude.with_config(ConfigFileLoader(config_path, config_profile))

    sent = amplitude.set_user_properties(
        data,
        # columns={'user_id': 'customer_id'},    # the row's id column, if named otherwise
    )
    # Events instead: a time column, and an event_type column or one name for all rows.
    # sent = amplitude.send_events(
    #     data,
    #     event_type='Order Completed',
    #     columns={'user_id': 'customer_id', 'time': 'ordered_at'},
    #     key_columns=['order_id'],              # what makes an event the same event
    # )
    print(f'{sent} rows sent to Amplitude')
