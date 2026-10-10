# R blocks

Data loaders, transformers, data exporters and custom blocks can be written in R. They run with R 4.6
and an R environment that [rv](https://github.com/A2-ai/rv) manages, without Docker. R
blocks and Python and SQL blocks pass data frames to each other in any order.

## Setup

```bash
pip install mage-ml               # Mage; R, rv and rig are installed outside Python
mage r setup path/to/project      # checks R, rv and rig, and prints how to install them
mage r init path/to/project       # creates the project's R environment
mage r status path/to/project     # checks that R blocks can run
```

`mage r setup` lists what R blocks need and the commands that install it on your
platform:

- [rv](https://github.com/A2-ai/rv), the package manager of R environments:
  `brew install rv-r` on macOS, an install script on Linux, a zip on Windows.
- R 4.6. [rig](https://github.com/r-lib/rig), the R installation manager, installs it:
  `rig add 4.6`. rig itself is `brew install r-rig` on macOS, a tarball on Linux and
  `winget install posit.rig` on Windows.

- On Linux, the system libraries that packages build with: rv builds a package from
  source when Posit Package Manager has no binary of it for the distribution and
  architecture, as for Debian on arm64. On Debian and Ubuntu, `mage r setup` prints the `apt-get install`
  command for the missing ones, and `rv sysdeps --only-absent` lists those of any rv
  environment. Elsewhere, install the development packages of cmake, curl, fontconfig,
  freetype, fribidi, harfbuzz, ICU, libjpeg, MariaDB, libpng, libpq, OpenSSL, libtiff,
  libuv, libwebp, libxml2 and zlib. The Docker image has them. arrow built from
  source leaves out S3 and GCS unless `LIBARROW_MINIMAL=false`, which `mage r init` and
  `mage r sync` set and the Docker image sets for `rv sync`.

`mage r init --install-r` runs `rig add` when the R version is missing.

### R versions

The R environment sets its R version (`r_version` in `rproject.toml`, 4.6 by default;
`mage r init --r-version 4.5` for another). R blocks run with the `Rscript` on `PATH`
when it is that version. Otherwise Mage uses the matching R version that rig installed,
and runs rv with it, so several projects on one machine can use different R versions.
`MAGE_RSCRIPT` overrides both.

On Linux and Windows, the versions rig installs run side by side. On macOS, R versions
share one framework and only the default one runs; switch it with `rig default 4.5`.
`mage r status` and the error of a block name the R versions rig has and the command to
use.

## The R environment

A project's R environment is an rv project: `<project>/r/rproject.toml` lists the R
version, the package repositories and the packages, `rv.lock` pins their versions, and
`rv sync` installs them into `<project>/r/rv/library`. You define it as you would any rv
project: edit `rproject.toml`, or use rv, then `mage r sync`:

```bash
cd path/to/project/r
rv add pointblank janitor                       # packages from CRAN or Posit Package Manager
rv add --git https://github.com/org/pkg.git pkg # or from git
```

`mage r init` starts it with the tidyverse, the packages Mage exchanges data with
(`arrow`, `bit64`, `jsonlite`, `tibble`, which every R environment needs), and what the
templates use: `DBI`, `RPostgres`, `RMariaDB`, `duckdb`, `RSQLite` and `httr2`.
`mage r init -p dplyr -p readr` starts with other packages instead of the tidyverse and
the template packages. `MAGE_R_PROJECT_DIR` points at an rv project elsewhere.

Commit `rproject.toml` and `rv.lock`; `rv/library` is ignored. On another machine,
`mage r sync` installs the locked versions.

R blocks see the rv library, R's base packages and Mage's `mageml` package; site and user
libraries are left out, so a pipeline runs with the locked packages only.

The Docker image has R 4.6 and rv. rv installs binaries from Posit Package Manager where
it has them, as for amd64 Linux, and builds the other packages from source, as on arm64
Linux. On macOS, Posit Package Manager has no R 4.6 binary of RMariaDB, so `mage r init`
takes it from CRAN, which has.

## Templates

In the notebook, add a block and choose R: data loaders, transformers and data exporters
each offer a generic template and these, each with a test; custom blocks offer a generic
template:

| Block | Templates |
| --- | --- |
| Data loader | Local file, Amazon S3, API, PostgreSQL, MySQL, DuckDB, SQLite |
| Transformer | Clean data, Aggregate, Join, Reshape |
| Data exporter | Local file, Amazon S3, API, PostgreSQL, MySQL, DuckDB, SQLite |
| Custom | Generic |

## Writing blocks

R has no decorators. Like plumber, Mage reads comments that start with `#*`:

```r
library(tidyverse)

#* @transformer
add_totals <- function(orders, customers, ...) {
  orders |>
    left_join(customers, by = "customer_id") |>
    mutate(total = price * quantity)
}

#* @test
totals_are_positive <- function(output) {
  stopifnot("A total is negative" = all(output$total >= 0, na.rm = TRUE))
}
```

- `#* @data_loader`, `#* @transformer`, `#* @data_exporter` or `#* @custom` marks the
  block's function. It receives the outputs of the upstream blocks in order, data frames
  as tibbles. A data loader, transformer or custom block returns its output; a custom
  block can return a data frame, a list, a value or nothing.
- `#* @test` marks a test. Mage calls it with the block's output after the block runs.
  A test fails when it raises an error, as `stopifnot()` and testthat's expectations do.
  The output is stored first, then failed tests fail the block, as with Python's `@test`.
  Tests are reported with their function names.
- A misspelled annotation, such as `#* @tset` or `#' @test`, is an error, so a test is
  never left out silently.

The annotations are comments, so a block file runs in any R session, where its functions
can be called and tested. `transformer(f)`, `test(f)` and the others register functions
from code. Blocks that define `load_data()`, `transform()` or `export_data()` by name, as
earlier versions of Mage required, still run.

The `mageml` package that runs blocks also gives them:

| Function | |
| --- | --- |
| `variable(name, default)` | A pipeline variable, such as `execution_date`. Without a default, a missing variable is an error. All variables are also in `global_vars` |
| `context()` | The block's UUID and type, the pipeline's UUID and the execution partition |
| `read_sql(query, database, profile, params)` | A query on a database of `io_config.yaml`, as a tibble |
| `write_table(df, table, database, profile, schema, if_exists)` | Writes a data frame to a table, replacing, appending or failing when it exists |
| `db_connect(database, profile)` | A DBI connection; close it with `DBI::dbDisconnect()` |
| `io_config(profile)` | A profile's database settings |
| `read_file(path, format)`, `write_file(df, path, format)` | A local CSV, TSV, Parquet, Feather/Arrow, JSON, NDJSON or RDS file, in the format its extension tells |
| `read_s3(uri, profile, format)`, `write_s3(df, uri, profile, format)` | The same on S3, such as `s3://bucket/orders.parquet` |
| `s3_filesystem(profile)` | An arrow S3 file system with a profile's AWS settings |

The help pages are in R: `?mageml::write_table`.

## Databases

`read_sql`, `write_table` and `db_connect` use the settings of a profile in the project's
`io_config.yaml`, with environment variables and secrets resolved by Mage. Postgres needs
the RPostgres package, MySQL RMariaDB and DuckDB duckdb.

```r
#* @data_loader
load_orders <- function(...) {
  read_sql(
    "SELECT * FROM orders WHERE day = $1",
    database = "postgres",
    profile = "warehouse",
    params = list(as.Date(variable("execution_date")))
  )
}
```

Mage passes the database and AWS settings of the profiles whose names appear in the
block as text, and of `default`; other settings are left out. The file
that holds them is readable by its owner only and is removed after the block runs.

`write_table` writes dates and date-times as ISO 8601 text with the column types that
read it. RPostgres writes the year 1 as `1-01-01`, which PostgreSQL reads as 2001, and
truncates microseconds, so a date-time a double held just below a microsecond lost it.

## Tests with pointblank

[pointblank](https://rstudio.github.io/pointblank/) validates data frames. Its
validation functions, applied to a data frame, stop at the first step that fails, so they
work in a test as they are:

```r
library(pointblank)

#* @test
orders_are_valid <- function(output) {
  output |>
    col_vals_not_null(order_id) |>
    rows_distinct(order_id) |>
    col_vals_between(quantity, 1, 1000)
}
```

The Python package works the same way in the `@test` functions of Python blocks;
`mage-ml[pointblank]` installs it:

```python
import pointblank as pb


@test
def test_orders(output, *args) -> None:
    (
        pb.Validate(data=output)
        .col_vals_not_null(columns='order_id')
        .rows_distinct(columns_subset='order_id')
        .interrogate()
        .assert_passing()
    )
```

`assert_passing()` raises an `AssertionError` that lists every failed step.

## Types

Data frames cross between Python and R as Arrow IPC files; other values cross as JSON.

| Python (pandas or Polars) | R | Back in Python |
| --- | --- | --- |
| integers that fit R's integers | integer | Int64 |
| other int64, uint64 below 2**63 | `bit64::integer64` | Int64 |
| float | double | float64 |
| bool | logical | boolean |
| str | character | str |
| category, Polars Categorical | factor | category |
| date | Date | date32[pyarrow] |
| naive datetime | POSIXct, shown in UTC | naive datetime64[us] |
| zoned datetime | POSIXct in its time zone | datetime64[us, zone] |
| timedelta | difftime | timedelta64[us] |
| time | hms | time64[us][pyarrow] |
| list | list | list[pyarrow] |
| bytes | arrow_binary | bytes |
| dict, struct, map, mixed values | list, from JSON | Python objects |
| decimal | double | float64 |
| UUID | character | str |

R blocks return tibbles, data frames, other values, which cross as JSON, or `NULL`, for no
output. Pipeline variables cross as JSON. Integers that a double cannot hold cross JSON as
`{"$int64": "<digits>"}`, which `mageml` reads as `integer64`.

### What R cannot hold

- **Decimals** become doubles, with 15 to 17 significant digits. Mage prints a warning
  for decimal columns with more than 15 digits.
- **-2**31** is R's `NA_integer_` and **-2**63** is bit64's NA, so they become NA. Mage
  sends -2**31 as `integer64`; RPostgres reads an int4 of -2**31 as NA. Mage warns about
  columns that hold -2**63.
- **NaN and NA** are one missing value once a frame crosses pandas between two blocks.
- **Date-times keep microseconds up to the year 2242**, since R holds them as doubles of
  seconds. `9999-12-31 23:59:59.999999`, a common value for "no end", is kept.
- **JSON arrays of numbers and text** become character vectors in R, as jsonlite
  simplifies them.
- **Nanoseconds** become microseconds.
- RPostgres reads `numeric` as double, `timestamp` as a UTC date-time, and intervals,
  JSON, arrays and enums as text.

## Files and S3

`read_file` and `write_file` read and write local files, with relative paths from the
project directory, which is the working directory of R blocks. CSV and TSV go through
readr, Parquet, Feather/Arrow and NDJSON through arrow, JSON arrays through jsonlite (with
every digit; jsonlite's default rounds to 4 significant digits), and RDS through
`readRDS`. `read_s3` and `write_s3` do the same on S3, with the `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION` and `AWS_ENDPOINT` settings of
an io_config.yaml profile; `AWS_ENDPOINT` points at S3-compatible storage such as MinIO.
Without them, arrow finds the credentials as the AWS tools do.

```r
#* @data_exporter
export_orders <- function(orders, ...) {
  write_s3(orders, "s3://warehouse/orders/2024.parquet", profile = "default")
}
```

## Settings

Environment variables:

| Variable | |
| --- | --- |
| `MAGE_R_PROJECT_DIR` | The rv project. Defaults to `<project>/r`, then to the project directory, wherever an `rproject.toml` is |
| `MAGE_RSCRIPT`, `MAGE_RV` | The Rscript and rv executables. Default to the ones on `PATH`; when the `Rscript` on `PATH` is not the environment's R version, the matching one of rig |
| `MAGE_RIG` | The rig executable. Defaults to the one on `PATH` |
| `MAGE_R_SYNC` | `check` (default) fails a block when the library is not synced with `rv.lock`; `auto` runs `rv sync` first; `off` skips the check |
| `MAGE_R_TIMEOUT` | Seconds after which an R block is stopped with its child processes. No limit by default |
| `MAGE_R_CACHE_DIR` | Where Mage installs `mageml`. Defaults to `~/.cache/mage-ml/r` |
| `MAGE_R_TZ` | The time zone of R sessions. Defaults to UTC |

Without an rv project, R blocks use the library of the Rscript they run with, which needs
the exchange packages.

## Errors

An R error fails the block with R's message and the block's calls, with their lines:

```
R block checker failed: expected 5 rows, got 1

R calls:
block.R#2: validate(df_1)
block.R#6: stop("expected 5 rows, got ", nrow(df))
```

Before a block runs, Mage checks that Rscript runs the R version of `rproject.toml`, that
the library is synced with `rv.lock`, and that the exchange packages are installed, and
says how to fix what is not. A missing package names the `rv add` command that installs
it. Printed output, messages and warnings reach the block's logs as they come.

## Speed

An R block starts an `Rscript` process: about 0.3 seconds for R and arrow, and 0.3 more
when the block loads the tidyverse. The checks of the environment take 0.3 seconds once
per Mage process, and again when `rproject.toml`, `rv.lock` or the library change. The
data crosses as uncompressed Arrow: a million rows add no measurable time. The first run
after an upgrade installs `mageml`, in a few seconds. Pipelines without R blocks never
start R.

## Design

- `mage_ai/data_preparation/models/block/r/__init__.py`: `RBlock` writes a job
  directory, runs `runner.R` with Rscript, reads the output and the test results, and
  replays the tests through Mage's `run_tests`.
- `runtime.py`: the settings, the checks of the rv environment, `mage r init`, and the
  installation of `mageml` into a cache keyed by its content and the R version, outside
  the rv library, which `rv sync` replaces with exactly the locked packages.
- `exchange.py`: what Python writes for R and reads back.
- `mageml/`: the R package that runs blocks, documented with roxygen2 and tested with
  testthat. `integration_tests/r/test_mageml_package.py` runs its tests, lintr with the
  tidyverse style guide, `R CMD check`, and checks that its documentation is generated.

Tests: `mage_ai/tests/data_preparation/models/block/r/` (Python, no R needed), the
package's testthat tests, and `integration_tests/r/` with R 4.6, rv and PostgreSQL:
every type round trip, errors, timeouts, the environment checks, the CLI, the notebook, and pipelines
that chain Python, R, Polars and SQL blocks and write to PostgreSQL with SQL exporters
and with DBI. `make -C integration_tests test-r` runs them.
