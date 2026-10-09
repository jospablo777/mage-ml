import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame, **kwargs) -> None:
    # The SQL block hands its result to the next block as a pandas frame.
    assert isinstance(frame, pd.DataFrame), type(frame)
    assert len(frame) == int(kwargs['expected_rows'])
    assert 'text_length' in frame.columns
    # Integer columns with NULLs stay integers; read_sql made them float64, which
    # rounds values above 2**53.
    for column in ('c_tinyint', 'c_smallint', 'c_integer', 'c_bigint', 'text_length'):
        assert str(frame[column].dtype) in ('Int64', 'int64'), (column, frame[column].dtype)
    assert frame['c_bigint'].max() == 2**63 - 1
