from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """A batch of Kafka messages, each a dict decoded from JSON."""
    return [dict(message, doubled=message['n'] * 2) for message in messages]
