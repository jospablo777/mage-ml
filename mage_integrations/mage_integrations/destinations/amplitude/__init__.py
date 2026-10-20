"""
The Amplitude destination of data integration pipelines: sends rows to an Amplitude project
with the Batch Event Upload API, as events or as user properties.

The settings' streams map says how each stream is sent:

    streams:
      orders:
        event_type: Order Completed   # or an event_type column
        time: created_at              # the column of the event's time
        user_id: customer_id          # the column of the user's id
      customer_scores:
        user_properties: true         # each row sets its user's properties

A stream that is not listed sends events when it has an event_type column. Amplitude's
other event fields and the event properties come from the columns (see
mage_ai.io.amplitude.amplitude_event).

Each event's insert_id is a hash of the stream's key properties (or of the event), so a
sync that runs again, or a request sent again, adds no event twice within Amplitude's 7
days of deduplication.
"""
import argparse
import sys
from typing import Dict, List

from mage_ai.io.amplitude import Amplitude as AmplitudeClient
from mage_ai.io.amplitude import AmplitudeError, amplitude_event
from mage_integrations.destinations.base import Destination


class Amplitude(Destination):
    _client = None

    @property
    def client(self) -> AmplitudeClient:
        if self._client is None:
            self._client = AmplitudeClient(
                api_key=self.config.get('api_key'),
                region=self.config.get('region') or 'US',
                host=self.config.get('host') or None,
                verbose=False,
                log=self.logger.info,
            )
        return self._client

    def test_connection(self) -> None:
        self.client.check_api_key()

    def stream_settings(self, stream: str) -> Dict:
        """How a stream is sent: as user properties, or as events of which type."""
        settings = dict(((self.config.get('streams') or {}).get(stream)) or {})
        columns = {
            field: settings[field] for field in ('user_id', 'device_id', 'time', 'insert_id')
            if settings.get(field)
        }
        if settings.get('user_properties'):
            return dict(identify=True, columns=columns)
        properties = (self.schemas.get(stream) or {}).get('properties') or {}
        if not settings.get('event_type') and 'event_type' not in properties:
            raise AmplitudeError(
                f'Stream {stream} has no event_type column. Name its events in the '
                f'settings (streams: {{{stream}: {{event_type: Order Completed}}}}), or set '
                f'its rows as user properties (streams: {{{stream}: '
                '{user_properties: true}}).'
            )
        return dict(event_type=settings.get('event_type'), columns=columns)

    def export_batch_data(self, record_data: List[Dict], stream: str, tags: Dict = None) -> None:
        settings = self.stream_settings(stream)
        keys = (self.key_properties or {}).get(stream) or \
            (self.unique_constraints or {}).get(stream) or None
        if settings.get('identify'):
            keys = None
        events = [
            amplitude_event(item['record'], key_columns=keys, **settings)
            for item in record_data
        ]
        sent = self.client.upload(events, min_id_length=self.config.get('min_id_length'))
        what = 'user property updates' if settings.get('identify') else 'events'
        self.logger.info(
            f'Sent {sent} {what} of stream {stream} to Amplitude.',
            tags=dict(**(tags or {}), sent=sent),
        )

if __name__ == '__main__':
    destination = Amplitude(
        argument_parser=argparse.ArgumentParser(),
        batch_processing=True,
    )
    destination.process(sys.stdin.buffer)
