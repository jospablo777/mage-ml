# Singer compatibility package

Source: https://github.com/mage-ai/singer-python, commit `0540a699c0e2fd8ba64c3245b5fa9aa87ae0538c`.

Tests use `assertEqual` for Python 3.12 compatibility. The source and tests retain the Apache 2.0 license. The local package replaces the upstream build metadata so jsonschema 4, current simplejson, and backoff 2 can resolve with Mage and dbt. Its version identifies the local build; it is not the public PyPI release.

Build and distribute this workspace package with `mage-integrations`. Run `uv run --no-sync pytest vendor/singer-python/tests` after dependency changes.
