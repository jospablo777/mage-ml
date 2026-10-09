import json
from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(messages: List[str], *args, **kwargs) -> List[Dict]:
    """A batch of ActiveMQ message bodies, as text."""
    rows = []
    for message in messages:
        data = json.loads(message)
        rows.append(dict(data, doubled=data['n'] * 2))
    return rows
