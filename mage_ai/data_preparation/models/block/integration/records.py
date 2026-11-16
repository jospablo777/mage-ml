from typing import Dict, List

import pandas as pd

# Block configuration: how many records of a stream one call of an integration
# pipeline's transformer receives. 1 calls it once per record.
CONFIG_KEY_RECORDS_PER_CALL = 'records_per_call'
# Large enough that the call costs nothing next to the work done on the frame, small
# enough to keep one frame of a batch (50,000 records by default) in memory at a time.
RECORDS_PER_CALL = 10_000


def records_per_call(configuration: Dict) -> int:
    value = (configuration or {}).get(CONFIG_KEY_RECORDS_PER_CALL)
    if value is None or value == '':
        return RECORDS_PER_CALL
    if isinstance(value, bool) or not str(value).strip().isdigit() or int(value) < 1:
        raise ValueError(
            f'{CONFIG_KEY_RECORDS_PER_CALL} must be a positive whole number, not {value!r}.',
        )
    return int(value)


def records_frame(records: List[Dict]) -> pd.DataFrame:
    """
    A frame of the records. A column of whole numbers with missing values stays whole
    numbers (Int64): pandas makes it float64, and 7 would reach the destination as 7.0.
    """
    frame = pd.DataFrame.from_records(records)
    for column in frame.columns[frame.dtypes == 'float64']:
        values = [record.get(column) for record in records]
        if all(value is None or type(value) is int for value in values):
            frame[column] = pd.array(values, dtype='Int64')
    return frame
