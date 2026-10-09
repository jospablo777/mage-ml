import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(model, *args, **kwargs) -> pl.DataFrame:
    # The model arrives through Mage's storage of scikit-learn outputs.
    features = pl.DataFrame({
        'id': list(range(10)),
        'x1': [float(i) for i in range(10)],
        'x2': [float(i % 4) for i in range(10)],
    })
    predictions = model.predict(features.select('x1', 'x2').to_pandas())
    return features.with_columns(prediction=pl.Series(predictions))
