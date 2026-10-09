from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(changes: List[Dict], *args, **kwargs) -> List[Dict]:
    """Change stream documents; the inserted documents keep their ObjectId."""
    return [
        dict(change['fullDocument'], doubled=change['fullDocument']['n'] * 2)
        for change in changes
        if change['operationType'] == 'insert'
    ]
