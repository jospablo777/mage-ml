"""
Checks the frame a MySQL SQL loader returns: integer columns with NULLs stay integers.
"""
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def check(frame: pd.DataFrame, **kwargs) -> pd.DataFrame:
    assert len(frame) == int(kwargs['expected_rows']), len(frame)
    for column in ('c_tinyint', 'c_smallint', 'c_mediumint', 'c_int', 'c_bigint'):
        assert str(frame[column].dtype) == 'Int64', (column, frame[column].dtype)
    for column in ('c_uint', 'c_ubigint'):
        assert str(frame[column].dtype) == 'UInt64', (column, frame[column].dtype)
    assert frame['c_bigint'].max() == 2**63 - 1
    assert frame['c_ubigint'].max() == 2**64 - 1
    return frame
