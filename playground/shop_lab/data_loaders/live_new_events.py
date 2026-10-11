import math
from datetime import datetime, timezone

import numpy as np
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def live_new_events(*args, **kwargs):
    """
    The web events of the last minute. Traffic rises and falls over an hour, so the
    service's dashboard shows runs of different sizes.
    """
    now = datetime.now(timezone.utc)
    rng = np.random.default_rng()
    wave = 1 + 0.6 * math.sin(2 * math.pi * now.minute / 60)
    rows = int(rng.poisson(300 * wave))
    seconds = rng.uniform(0, 60, rows)
    event_type = rng.choice(['page_view', 'search', 'add_to_cart', 'checkout', 'purchase'],
                            rows, p=[0.62, 0.15, 0.12, 0.06, 0.05])
    return pd.DataFrame({
        'occurred_at': pd.Timestamp(now) - pd.to_timedelta(seconds, unit='s'),
        'event_type': event_type,
        'device': rng.choice(['desktop', 'iphone', 'android', 'tablet'], rows,
                             p=[0.45, 0.25, 0.22, 0.08]),
        'amount': np.where(event_type == 'purchase', rng.gamma(2.0, 25.0, rows).round(2), 0.0),
    })
