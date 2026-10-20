"""The features of MageML's `mage` crate that a test project's Rust blocks build with."""


def enable(project: str, *features: str):
    """Turns on features of the mage crate in the project's rust/Cargo.toml."""
    from mage_ai.data_preparation.models.block.rust import workspace

    rust = workspace.init(project)
    text = rust.cargo_toml.read_text()
    wanted = ['postgres', *features]
    line = 'features = [' + ', '.join(f'"{name}"' for name in wanted) + ']'
    for existing in ('features = ["postgres"]', line):
        if existing in text:
            text = text.replace(existing, line)
            break
    else:
        raise AssertionError(f'{rust.cargo_toml} has no mage-block features to extend')
    rust.cargo_toml.write_text(text)
    return rust
