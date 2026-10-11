"""
Writes the ONNX models the onnx tests read. Run from this folder with:

    uv run --no-project --with onnx --with skl2onnx --with scikit-learn --with pandas \
        python make_models.py
"""
import numpy as np
import pandas as pd
from skl2onnx import to_onnx
from skl2onnx.common.data_types import FloatTensorType, StringTensorType
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


def write(name, model):
    with open(name, 'wb') as file:
        file.write(model.SerializeToString())


# y = 3 x0 - 2 x1 + 1, exactly.
X = np.array([[0, 0], [1, 0], [0, 1], [2, 3]], dtype=np.float32)
regressor = LinearRegression().fit(X, 3 * X[:, 0] - 2 * X[:, 1] + 1)
write('regressor.onnx', to_onnx(regressor, X[:1]))

# Class 1 when x0 > x1. With zipmap the probabilities are a list of maps.
Xc = np.array([[0, 5], [1, 4], [5, 0], [4, 1], [0, 3], [3, 0]], dtype=np.float32)
classifier = LogisticRegression(C=100).fit(Xc, (Xc[:, 0] > Xc[:, 1]).astype(np.int64))
write('classifier.onnx', to_onnx(classifier, Xc[:1], options={id(classifier): {'zipmap': False}}))
write('classifier_zipmap.onnx', to_onnx(classifier, Xc[:1]))

# One input per column: the city one-hot encoded, the amount passed through.
frame = pd.DataFrame({'city': ['paris', 'lima', 'paris', 'lima'], 'amount': [1.0, 2.0, 3.0, 4.0]})
pipeline = Pipeline([
    ('features', ColumnTransformer([
        ('city', OneHotEncoder(), ['city']),
        ('amount', 'passthrough', ['amount']),
    ])),
    ('model', LinearRegression()),
]).fit(frame, [10.0, 20.0, 30.0, 40.0])
write('pipeline.onnx', to_onnx(pipeline, initial_types=[
    ('city', StringTensorType([None, 1])), ('amount', FloatTensorType([None, 1])),
]))
