from typing import Dict, List

SESSION_MS = 30 * 60 * 1000

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def sessionize(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """
    Each click with the session it belongs to, without state: the user and the 30-minute
    window of its time. The sink upserts by event_id, so a replayed batch writes nothing twice.
    """
    return [
        dict(event_id=m['event_id'], user_id=m['user'], page=m['page'], ts=m['ts'],
             session_key=f"{m['user']}-{m['ts'] // SESSION_MS}")
        for m in messages
    ]
