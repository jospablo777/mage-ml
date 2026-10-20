"""The features of MageML's `mage` crate that a test project's Rust blocks build with."""
import contextlib
import re

MAGE_BLOCK = re.compile(r'^(mage-block = \{[^\n]*features = \[)([^\]]*)(\][^\n]*)$', re.MULTILINE)


@contextlib.contextmanager
def enabled(project: str, *features: str):
    """
    Turns on features of the mage crate in the project's rust/Cargo.toml for one test, and
    restores the file after it: tests share the project, and a feature left on made later
    tests rebuild Polars with it (the cloud clients of storage) past their time limits.
    """
    from mage_ai.data_preparation.models.block.rust import workspace

    rust = workspace.init(project)
    original = rust.cargo_toml.read_text()
    match = MAGE_BLOCK.search(original)
    if not match:
        raise AssertionError(f'{rust.cargo_toml} has no mage-block features to extend')
    present = [f.strip().strip('"') for f in match.group(2).split(',') if f.strip()]
    wanted = present + [f for f in features if f not in present]
    listed = ', '.join(f'"{name}"' for name in wanted)
    rust.cargo_toml.write_text(
        original[:match.start()] + match.group(1) + listed + match.group(3)
        + original[match.end():],
    )
    try:
        yield rust
    finally:
        rust.cargo_toml.write_text(original)
