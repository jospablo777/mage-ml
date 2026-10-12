import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(trips, *args, **kwargs):
    when = pd.Timestamp(kwargs['execution_date']).tz_localize('UTC').floor('s')
    return pd.DataFrame({
        'driver_id': trips['driver_id'].astype('int64'),
        'conv_rate': (trips['accepted'] / trips['offered']).astype('float32'),
        'acc_rate': (trips['accepted'] / trips['trips']).astype('float64'),
        'avg_daily_trips': trips['trips'].astype('int64'),
        'city': 'Heredia',
        'active': True,
        'event_timestamp': when,
        'created': when,
    })


@test
def test_rates_are_fractions(output, *args) -> None:
    assert output['conv_rate'].between(0, 1).all()
