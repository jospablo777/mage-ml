"""
The MLflow run context provider of Mage (see experiments.py). MLflow loads it through
the `mlflow.run_context_provider` entry point, so this module imports MLflow.
"""
from mlflow.tracking.context.abstract_context import RunContextProvider

from mage_ai.orchestration.experiments import current_tags


class MlflowRunContext(RunContextProvider):
    """Tags MLflow runs started inside a Mage block with the Mage run that runs it."""

    def in_context(self) -> bool:
        return bool(current_tags())

    def tags(self):
        return dict(current_tags())
