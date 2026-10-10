class AtlasError(Exception):
    """A ColumnAtlas request failed; the message is safe to show to users."""

    status = 500


class AtlasInvalidRequest(AtlasError):
    status = 400


class AtlasUnavailable(AtlasError):
    """The output cannot be explored: not stored, not a dataframe, or not a local file."""

    status = 404


class AtlasTimeout(AtlasError):
    status = 504


class AtlasBusy(AtlasError):
    status = 503


class AtlasEngineError(AtlasError):
    status = 500
