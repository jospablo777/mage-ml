from mage_ai.feature_stores.feast import FeastClient
from mage_ai.pipeline_services.models import feature_service

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(pending, *args, **kwargs):
    with FeastClient() as feast:
        return feast.get_online_features(
            entities={'driver_id': pending['driver_id'].astype('int64').tolist()},
            feature_service=feature_service('churn'),
        )
