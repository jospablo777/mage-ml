from integration_tests.data import r_dataset

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    return r_dataset.source_frame(int(kwargs.get('rows', 200)))
