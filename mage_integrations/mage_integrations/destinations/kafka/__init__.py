import argparse
import ast
import json
import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from typing import Dict, List

from kafka import KafkaConsumer, KafkaProducer
from mage_integrations.destinations.base import Destination
from mage_integrations.destinations.constants import KEY_RECORD
from mage_integrations.destinations.utils import update_record_with_internal_columns


# Seconds a send waits for Kafka to acknowledge it.
SEND_TIMEOUT_SECONDS = 60


class Kafka(Destination):
    def _api_version(self):
        """
        The configured protocol version, or None to detect the broker's. (0, 10, 2), the
        default before, made every request time out against Kafka 4.
        """
        version = self.config.get('api_version')
        return ast.literal_eval(version) if isinstance(version, str) else version

    def test_connection(self) -> None:
        consumer = KafkaConsumer(
            bootstrap_servers=self.config['bootstrap_server'],
            api_version=self._api_version(),
        )
        topics = consumer.topics()
        consumer.close()
        if not topics:
            raise Exception('Kafka was not able to connect to BootStrap Server')
        return True

    def build_client(self):
        kwargs = dict(
            bootstrap_servers=self.config['bootstrap_server'],
            api_version=self._api_version(),
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x is not None else None,
        )
        producer = KafkaProducer(**kwargs)
        return producer

    def export_batch_data(self, record_data: List[Dict], stream: str, tags: Dict = None) -> None:

        self.logger.info('Export data started.')

        producer = self.build_client()

        if self.key_properties.get(stream) is not None and len(self.key_properties[stream]) >= 1:
            key_property = self.key_properties[stream][0]
        else:
            key_property = None

        self.logger.info('Inserting records started.')

        futures = []
        for r in record_data:
            r[KEY_RECORD] = update_record_with_internal_columns(r[KEY_RECORD])
            # The key is the record's value of the key property. The property's name was
            # sent as the key of every record, so they all went to one partition.
            key = r[KEY_RECORD].get(key_property) if key_property else None
            futures.append(producer.send(
                self.config['topic'],
                r[KEY_RECORD],
                key=None if key is None else str(key),
            ))
        # The records stayed in the producer's buffer when the batch ended, and failed
        # sends went unnoticed.
        producer.flush(timeout=SEND_TIMEOUT_SECONDS)
        for future in futures:
            future.get(timeout=SEND_TIMEOUT_SECONDS)
        producer.close()

        self.logger.info('Export data completed')


if __name__ == '__main__':
    destination = Kafka(
        argument_parser=argparse.ArgumentParser(),
        batch_processing=True,
    )
    destination.process(sys.stdin.buffer)
