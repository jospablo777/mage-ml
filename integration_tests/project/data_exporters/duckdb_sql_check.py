import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame, **kwargs) -> None:
    # The SQL block hands its result to the next block as a pandas frame.
    assert isinstance(frame, pd.DataFrame), type(frame)
    assert len(frame) == int(kwargs['expected_rows'])
    assert 'text_length' in frame.columns
