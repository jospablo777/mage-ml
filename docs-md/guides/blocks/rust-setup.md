# Set up Rust

*Install Rust next to MageML on your machine, build your first Rust block and keep the build cache in check.*

Rust blocks are compiled into native programs that run Polars. The MageML Docker image
already has Rust and the dependencies prebuilt. This page is for MageML installed in your
own environment, on macOS or Linux.

You need:

| Tool | What it does |
| --- | --- |
| [rustup](https://rustup.rs) | Installs Rust; reads the version each project pins |
| A C toolchain | Links the programs (Xcode Command Line Tools on macOS, `build-essential` on Debian and Ubuntu) |

Installing MageML with uv also needs Rust, because it builds `column-atlas-native`, a Rust
package of the repository. So set up Rust first.

> [!NOTE]
> MageML runs Python blocks with no setup. When you create a Rust block on a machine where
> Rust is not ready, the editor says what is missing, with the commands below and a link
> to this page.

## 1. Install the C toolchain

**macOS**

```bash
xcode-select --install
```

**Debian and Ubuntu**

```bash
sudo apt install build-essential pkg-config
```

## 2. Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Accept the defaults, then open a new terminal (or run `source "$HOME/.cargo/env"`) and check:

```bash
cargo --version
```

Each project pins its Rust version in `rust/rust-toolchain.toml` (1.98.0 for new
projects); rustup downloads that version on the first build, so you do not install it by
hand.

## 3. Install MageML with uv

```bash
git clone https://github.com/jospablo777/mage-ml.git
cd mage-ml
uv sync --locked --no-default-groups
```

Or into a Python 3.12 environment you already use:

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python "mage-ml @ git+https://github.com/jospablo777/mage-ml"
```

The install from GitHub compiles the Rust package and takes about three minutes. Add the
extras of the databases your blocks use, such as `"mage-ml[r,rust,postgres] @ git+..."`;
the `rust` extra adds no packages, since Rust comes from rustup.

## 4. Check the setup

```bash
mage rust status path/to/project
```

```text
cargo: cargo 1.98.0 (797e8a9bc 2026-08-05)
rustc: rustc 1.98.0 (88d9e12ae 2026-08-18)
workspace: not created
target directory: /Users/me/.cache/mage/rust/target
Rust blocks: 0
Rust blocks can build.
```

The workspace is created with the first build, or now with `mage rust init path/to/project`.
It lives in `<project>/rust`:

| File | |
| --- | --- |
| `rust/Cargo.toml` | The crates every block can use, under `[workspace.dependencies]` |
| `rust/Cargo.lock` | The resolved versions |
| `rust/rust-toolchain.toml` | The Rust version |
| `rust/.mage/` | Generated: MageML's `mage` crate and one crate per block; do not edit or commit |

Commit the first three.

## 5. Build and run your first Rust block

Start MageML, open a pipeline, add a transformer and select **Rust**. Or write the block in
`transformers/revenue_by_region.rs`:

```rust
use mage::prelude::*;

fn transform(orders: LazyFrame) -> Result<LazyFrame> {
    Ok(orders
        .group_by([col("region")])
        .agg([col("amount").sum().alias("revenue"), len().alias("orders")])
        .sort(["region"], Default::default()))
}

fn test(output: &DataFrame) -> Result<()> {
    anyhow::ensure!(output.height() == 3, "expected 3 regions, got {}", output.height());
    Ok(())
}
```

The upstream block's table arrives as a `LazyFrame`: Polars reads only what the query uses
and optimizes the whole query before it runs.

Build every Rust block of the project ahead of time:

```bash
mage rust build path/to/project
```

```text
✓ transformers/revenue_by_region.rs: built in 132.9 s
All 1 Rust blocks are built.
```

The first build of a machine compiles Polars and its dependencies: about 2 minutes on a
14-core laptop, longer on smaller machines. After that a block compiles in about a second:

```text
✓ transformers/revenue_by_region.rs: built in 1.2 s
```

You do not have to run `mage rust build`: a block builds when it first runs, and MageML
compiles the pipelines after each save and at server start. The command is useful before a
deployment, or to see every compiler error at once.

While you type in the editor, MageML checks the block with the Rust compiler and underlines
errors and warnings on their lines.

## Add crates

Add a crate under `[workspace.dependencies]` in `rust/Cargo.toml`, then use it in any block:

```toml
[workspace.dependencies]
regex = "1"
```

New projects come with `anyhow`, `serde`, `serde_json`, `rayon` and `ureq` (HTTP with
rustls). Polars comes with the `mage` crate; to enable more Polars features, add `polars`
with the same version and the features you need. See [Rust blocks](rust-blocks.md#crates).

## The build cache

Compiled code goes to one directory shared by every project on the machine, so a second
project reuses the compiled Polars:

| Variable | Default | |
| --- | --- | --- |
| `MAGE_RUST_TARGET_DIR` | `~/.cache/mage/rust/target` | Where compiled code goes |
| `MAGE_RUST_BINARY_CACHE_MB` | `5000` | Size of the cache of compiled blocks |
| `MAGE_RUST_BUILD_TIMEOUT_SECONDS` | `1800` | Stops a build that takes longer |

A first build takes about 1.3 GB. The directory grows as crates, Rust versions and projects
change; check it with `du -sh ~/.cache/mage/rust/target`. Deleting it is safe: the next
build compiles Polars again.

## Several machines and CI

The toolchain file and `Cargo.lock` make every machine build the same crates with the same
Rust. In CI, cache `~/.cache/mage/rust/target` (or the directory in `MAGE_RUST_TARGET_DIR`)
between runs, keyed on `rust/Cargo.lock`, to skip the first-build time.

Exported [pipeline services](../../production/pipeline-services.md) compile the Rust blocks when the
image builds, so the machine that runs the service needs no Rust.

## Troubleshooting

| Message | Fix |
| --- | --- |
| `Rust blocks need Rust` | Install rustup, then open a new terminal so `cargo` is on `PATH` |
| ``error: linker `cc` not found`` | Install the C toolchain (step 1) |
| A build takes minutes every time | Check that `MAGE_RUST_TARGET_DIR` points at a directory that persists |
| `Rust blocks can build` but a block fails to compile | Run `mage rust build path/to/project` to see every error with its line |

For block functions, inputs, outputs, PostgreSQL and templates, see
[Rust blocks](rust-blocks.md).
