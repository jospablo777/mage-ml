import pandas as pd
import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(frame, *args, **kwargs) -> pd.DataFrame:
    # The upstream block returned a LazyFrame; its output in S3 arrives as a scan.
    assert isinstance(frame, pl.LazyFrame), type(frame)
    result = frame.collect().to_pandas(use_pyarrow_extension_array=True)
    result['f64_doubled'] = result['f64'] * 2
    return result


@test
def test_arrow_types(frame, **kwargs) -> None:
    assert all(isinstance(dtype, pd.ArrowDtype) for dtype in frame.dtypes)
