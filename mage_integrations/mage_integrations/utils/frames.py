from typing import Dict, List

import pandas as pd


def records_from_frame(df: pd.DataFrame) -> List[Dict]:
    """
    Rows of df as dicts, with None for every missing value.

    pandas 3 stores missing text as NaN in the str dtype, and NA and NaT mark missing
    values in other dtypes. Singer records are JSON, where NaN is not a value: a
    destination would receive a float in a string field.
    """
    return df.astype(object).where(df.notna(), None).to_dict('records')
