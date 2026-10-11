"""A temporary MLflow tracking store for tests."""
import os
import shutil
import tempfile
from pathlib import Path


def use_temporary_mlflow(test_case, experiments=()) -> str:
    """
    Points MLflow at a SQLite store in a temporary folder for one test and returns its URI.
    MLflow's client gives the experiments it creates, Default included, the artifact root
    ./mlruns in the working directory, which is the checkout in a test run. The store and
    the experiments the test's blocks use are created here with their artifacts in the
    temporary folder. The previous tracking URI comes back after the test.
    """
    import mlflow
    from mlflow.store.tracking.sqlalchemy_store import SqlAlchemyStore

    folder = Path(tempfile.mkdtemp())
    test_case.addCleanup(shutil.rmtree, folder, True)
    uri = f'sqlite:///{folder}/mlflow.db'
    artifacts = (folder / 'artifacts').as_uri()
    store = SqlAlchemyStore(uri, artifacts)
    for name in experiments:
        store.create_experiment(name, artifact_location=f'{artifacts}/{name}')
    store.engine.dispose()

    previous = os.environ.get('MLFLOW_TRACKING_URI')

    def restore():
        mlflow.set_tracking_uri(previous)
        if previous is None:
            os.environ.pop('MLFLOW_TRACKING_URI', None)

    test_case.addCleanup(restore)
    os.environ['MLFLOW_TRACKING_URI'] = uri
    mlflow.set_tracking_uri(uri)
    return uri
