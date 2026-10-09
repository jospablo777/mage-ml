import polars as pl

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    # Polars 2 cannot write Binary columns to JSON and panics. A Rust panic arrives as
    # pyo3_runtime.PanicException, which derives from BaseException, not Exception.
    return pl.DataFrame({'payload': [b'\x00']}).write_json()
