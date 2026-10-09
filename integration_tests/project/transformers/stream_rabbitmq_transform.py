import json
from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(messages, *args, **kwargs) -> List[Dict]:
    """A batch of RabbitMQ messages, each a Payload(method, properties, body)."""
    rows = []
    for message in messages:
        data = json.loads(message.body)
        rows.append(dict(data, doubled=data['n'] * 2))
    return rows
