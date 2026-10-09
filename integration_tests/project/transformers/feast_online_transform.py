if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(frame, *args, **kwargs):
    offset = int(kwargs['driver_offset'])
    result = frame.copy(deep=False)
    result['driver_id'] = frame['driver_id'] + offset
    result['conv_rate'] = frame['conv_rate'] * 2
    result['avg_daily_trips'] = frame['avg_daily_trips'] + 1
    result['city'] = frame['city'].str.upper()
    result['active'] = ~frame['active']
    result['event_timestamp'] = kwargs['event_timestamp']
    result['created'] = kwargs['event_timestamp']
    return result
