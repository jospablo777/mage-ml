import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, data_2, *args, **kwargs):
    """
    Joins the outputs of two upstream blocks: data is the first, data_2 the second. The
    columns of the on variable (id by default) match their rows; how (left by default) is
    inner, left, right, full, semi or anti.
    """
    frames = []
    for frame in (data, data_2):
        frame = frame.collect() if isinstance(frame, pl.LazyFrame) else frame
        frames.append(frame if isinstance(frame, pl.DataFrame) else pl.from_pandas(frame))
    on = kwargs.get('on', 'id')
    return frames[0].join(frames[1], on=on, how=kwargs.get('how', 'left'), coalesce=True)


@test
def test_output_has_rows(output, *args) -> None:
    assert len(output) > 0, 'The output is empty'
