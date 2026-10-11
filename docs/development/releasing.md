# Releasing MageML

Releases are GitHub releases with wheels, and a package index on GitHub Pages
(`https://jospablo777.github.io/mage-ml/simple/`) that uv and pip install from. Nothing is
published to PyPI and no container image is published. The workflow is
`.github/workflows/release.yml`; it runs only by hand.

## Once per repository

1. Settings > Pages: set the source to **GitHub Actions**.
2. Settings > Environments: create `release` with required reviewers, so publishing waits
   for approval.
3. Settings > General > Releases: turn on release immutability.

## Each release

1. Set the version in `pyproject.toml` (and in `mage_integrations/pyproject.toml` or
   `rust/column_atlas/pyproject.toml` when their code changed; `mage-ml` pins both
   exactly), run `uv lock`, and move the release notes' "unreleased" section to the date.
2. Write `docs/development/releases/vX.Y.Z.md`, the text of the GitHub release.
3. Check locally: the unit tests, then build and qualify the wheels on this machine:

   ```bash
   (cd mage_ai/frontend && yarn export_prod && yarn export_prod_base_path)
   for p in mage-ml mage-integrations singer-python column-atlas-native; do
     uv build --package $p --wheel --no-sources --out-dir dist-release
   done
   python3 scripts/release/qualify_install.py dist-release --version X.Y.Z --report q.json
   ```

4. Commit, push, then tag that commit: `git tag vX.Y.Z && git push origin vX.Y.Z`.
5. Run **Release** from the Actions tab with the tag and `publish` off. It builds every
   wheel from the tag and qualifies the install on Linux x86-64, Linux ARM64 and macOS
   Apple Silicon. Read the qualification reports in the run's artifacts.
6. Run it again with `publish` on. After approval in the `release` environment it drafts
   the release, uploads and verifies the assets, publishes it and deploys the index.
7. Install from the public index in a fresh environment to confirm, then mark the release
   as latest.

## Supported platforms

Linux x86-64 and ARM64 with glibc 2.28 or newer, macOS 12 or newer on Apple Silicon,
CPython 3.12. macOS on Intel is excluded because cryptography no longer ships Intel macOS
wheels.
