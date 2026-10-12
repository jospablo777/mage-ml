# Set up R

*Install R and rv next to MageML on your machine, create the project's R environment and run your first R block.*

R blocks run R scripts with the packages of the project's R environment. The MageML Docker
image already has R 4.6 and rv. This page is for MageML installed in your own Python
environment, on macOS, Linux or Windows.

You need three things:

| Tool | What it does | Required |
| --- | --- | --- |
| R 4.3 or newer (4.6 by default) | Runs the blocks (`Rscript`) | Yes |
| [rv](https://github.com/A2-ai/rv) | Installs and locks the project's R packages | Yes |
| [rig](https://github.com/r-lib/rig) | Installs and switches R versions | No, recommended |

MageML does not use your R library or RStudio settings: each project gets its own package
library, so installing R for MageML does not change your other R work.

> [!NOTE]
> MageML runs Python blocks with no setup. When you create an R block on a machine where
> R is not ready, the editor says what is missing, with the commands below and a link
> to this page.

## 1. Install MageML with uv

If MageML is not installed yet, install it with [uv](https://docs.astral.sh/uv/). From a
clone of the repository:

```bash
git clone https://github.com/jospablo777/mage-ml.git
cd mage-ml
uv sync --locked --no-default-groups
uv run --no-sync mage --help
```

Or into a Python 3.12 environment you already use:

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python "mage-ml @ git+https://github.com/jospablo777/mage-ml"
```

Add the extras of the databases your blocks use, such as
`"mage-ml[r,rust,postgres] @ git+https://github.com/jospablo777/mage-ml"`. The `r` and
`rust` extras add no packages: R, rv and Rust are installed outside Python, as below.

> [!NOTE]
> Both routes build `column-atlas-native`, a Rust package of the repository, so they need
> Rust: install it with [rustup](https://rustup.rs) first (see [Rust setup](rust-setup.md)).
> The install from GitHub takes about three minutes. Plain `pip install` does not work yet,
> because `column-atlas-native` is not on PyPI.

## 2. Install R

Choose one. Both give you `Rscript`, which MageML runs.

**rig (recommended)**

rig installs R versions side by side and switches between them.

| System | Install rig |
| --- | --- |
| macOS | `brew install r-rig` |
| Linux | `` curl -Ls https://github.com/r-lib/rig/releases/download/latest/rig-linux-$(arch)-latest.tar.gz \| `which sudo` tar xz -C /usr/local `` |
| Windows | `winget install posit.rig` |

Then install R 4.6 and make it the default:

```bash
rig add 4.6
rig default 4.6
```

`mage r init --install-r` runs `rig add` for you when the R version is missing.

**CRAN**

Download R from CRAN, the R project's site: [cloud.r-project.org](https://cloud.r-project.org).

- **macOS:** the `.pkg` installer for your processor (arm64 for Apple silicon).
- **Windows:** "base", then the installer. Add R's `bin` folder (for example
  `C:\Program Files\R\R-4.6.1\bin`) to `PATH`, or set `MAGE_RSCRIPT` to the full path of
  `Rscript.exe`.
- **Debian and Ubuntu:** follow CRAN's instructions to add its apt repository, then
  `sudo apt install r-base r-base-dev`. The distribution's own R may be older than 4.3.

Check it:

```bash
Rscript --version
```

## 3. Install rv

| System | Install rv |
| --- | --- |
| macOS | `brew install rv-r` |
| Linux | `curl -sSL https://raw.githubusercontent.com/A2-ai/rv/refs/heads/main/scripts/install.sh \| bash` |
| Windows | Download the `x86_64-pc-windows-msvc` zip from [rv's releases](https://github.com/a2-ai/rv/releases/latest) and add its folder to `PATH` |

```bash
rv --version
```

## 4. Check the setup

`mage r setup` looks for rig, R and rv and prints the install command for anything missing:

```bash
mage r setup path/to/project
```

```text
✓ rig, the R installation manager (optional, recommended): /Users/me/.local/bin/rig
✓ R 4.6: /usr/local/bin/Rscript (R 4.6.1)
✓ rv, the R package manager: /opt/homebrew/bin/rv
R blocks can be set up. Create the R environment with `mage r init path/to/project`; ...
```

## 5. Create the project's R environment

```bash
mage r init path/to/project
```

This creates `<project>/r` with `rproject.toml` and `rv.lock` and installs the tidyverse,
the packages MageML exchanges data with (`arrow`, `bit64`, `jsonlite`, `tibble`), the
database drivers the templates use, and tidymodels with the `glmnet`, `ranger` and
`xgboost` engines and `bundle`. Most packages come as prebuilt binaries, so this takes
a minute or two.

To start with fewer packages:

```bash
mage r init path/to/project -p dplyr -p readr
```

Then check that R blocks can run:

```bash
mage r status path/to/project
```

The last line says `R blocks can run.` To add packages later, see
[R packages with rv](r-packages.md).

## 6. Run your first R block

Start MageML, open a pipeline, add a transformer and select **R**:

```bash
mage start path/to/project
```

Or write the block yourself. An R block is a function with an annotation; the upstream
block's output arrives as `df_1`, a data frame:

```r
library(dplyr)

#* @transformer
transform <- function(df_1, ...) {
  df_1 |>
    group_by(region) |>
    summarise(orders = n(), revenue = sum(amount))
}

#* @test
test_output <- function(output, ...) {
  stopifnot(nrow(output) > 0)
}
```

Run the pipeline from the command line with `mage run path/to/project <pipeline>`, or from the
editor. R blocks receive and return tables as Arrow, so they mix with Python, Rust and SQL
blocks in one pipeline. See [R blocks](r-blocks.md) for templates, variables,
databases and files.

## Several R versions

The R version of a project is `r_version` in `r/rproject.toml` (4.6 by default). MageML runs
the `Rscript` on `PATH` when its version matches, otherwise the matching version rig
installed.

- **Another version for a new environment:** `mage r init path/to/project --r-version 4.5`.
- **A specific Rscript:** set `MAGE_RSCRIPT=/path/to/Rscript`.
- **macOS with rig:** rig's R versions run only as the default one; switch with
  `rig default 4.5`.

## Settings

| Variable | Default | Description |
| --- | --- | --- |
| `MAGE_RSCRIPT` | `Rscript` on `PATH`, then rig's | The Rscript that runs R blocks |
| `MAGE_RV` | `rv` on `PATH` | The rv executable |
| `MAGE_R_PROJECT_DIR` | `<project>/r`, then `<project>` | The folder with `rproject.toml` |
| `MAGE_R_SYNC` | `check` | `check` stops R blocks when the library does not match `rv.lock`; `auto` runs `rv sync` first; `off` skips the check |

## Troubleshooting

| Message | Fix |
| --- | --- |
| `Rscript (Rscript) was not found` | Install R, add its `bin` folder to `PATH`, or set `MAGE_RSCRIPT` |
| `rv (rv) was not found` | Install rv, or set `MAGE_RV` |
| `The R environment in ... is for R 4.6, but Rscript runs R 4.5` | Install that version (`rig add 4.6` or `mage r init --install-r`), or change `r_version` in `rproject.toml` and run `mage r sync` |
| `The R library in ... is not synced with rv.lock` | Run `mage r sync path/to/project` |
| `ERROR: compilation failed for package ...` on Linux | Install the system library the package needs; see [System libraries on Linux](r-packages.md#system-libraries-on-linux) |
