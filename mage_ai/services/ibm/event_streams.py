"""
IBM Event Streams (Kafka on IBM Cloud) from its service credentials: the JSON IBM Cloud
makes for a service key, with the brokers (`kafka_brokers_sasl`) and the API key. A Kafka
source or sink sets `event_streams_credentials` to it, usually
"{{ env_var('EVENT_STREAMS_CREDENTIALS') }}", instead of the brokers and SASL settings.
"""
import json
from typing import Dict


class EventStreamsCredentialsError(ValueError):
    pass


def kafka_settings(credentials) -> Dict:
    """The Kafka settings the credentials stand for: brokers, SASL_SSL and PLAIN sign-in."""
    if isinstance(credentials, str):
        try:
            credentials = json.loads(credentials)
        except ValueError as error:
            raise EventStreamsCredentialsError(
                f'event_streams_credentials is not the service credentials JSON: {error}',
            )
    if not isinstance(credentials, dict):
        raise EventStreamsCredentialsError(
            'event_streams_credentials is the service credentials JSON object.',
        )
    brokers = credentials.get('kafka_brokers_sasl') or []
    if isinstance(brokers, str):
        brokers = [b for b in brokers.split(',') if b]
    if not brokers:
        raise EventStreamsCredentialsError(
            'The Event Streams credentials have no kafka_brokers_sasl.',
        )
    password = credentials.get('password') or credentials.get('api_key')
    if not password:
        raise EventStreamsCredentialsError('The Event Streams credentials have no api_key.')
    return dict(
        bootstrap_server=','.join(brokers),
        security_protocol='SASL_SSL',
        sasl_config=dict(
            mechanism='PLAIN',
            username=credentials.get('user') or 'token',
            password=password,
        ),
    )


def expand(config: Dict) -> Dict:
    """A Kafka config with event_streams_credentials replaced by the settings it stands for."""
    credentials = config.pop('event_streams_credentials', None)
    if credentials:
        config.update(kafka_settings(credentials))
    return config
