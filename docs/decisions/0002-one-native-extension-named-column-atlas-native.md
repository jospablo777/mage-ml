# 2. One native extension, named column-atlas-native

Date: 2026-10-20

Status: Accepted

## Context

`column-atlas-native` started as the Rust code of ColumnAtlas, the block output explorer:
Parquet queries and column profiles. Since then it took two more jobs that Python blocks and
the server call on every run:

- `postgres.rs`: PostgreSQL I/O for `mage_ai.io.postgres`, exports by binary `COPY` encoded
  in Rust and Polars loads by binary `COPY` (`column_atlas_native.pg_*`).
- `logs.rs`: block run logs read in windows over their rotated segments, and searched
  (`column_atlas_native.log_window`, `log_search`).

The name says ColumnAtlas; the package is MageML's native extension. Renaming it was
considered: `mageml-native`, importing `mageml_native`.

## Decision

Keep one native extension, and keep its name, `column-atlas-native`, imported as
`column_atlas_native`.

- **One extension, not several.** Each Rust extension is a wheel per platform to build, test
  and publish, and its own copy of Polars and Arrow in memory. The three jobs share Polars,
  Arrow and the build; split, they would load Polars twice in one process.
- **Not renamed.** It is a dependency of `mage-ml`, installed with it and called by MageML's
  own code: no user installs it, imports it or writes its name. A new name would change the
  crate, every import, the CI jobs, the release scripts and the package index, and keep
  the old name published beside it, for a name nobody types.
- **Its description says what it is.** The package description is "Native code of MageML",
  and each module says what it serves.

## Consequences

- New native code that Python calls (readers, encoders, profilers) goes into this
  extension, as a module of its own (`src/<job>.rs` and its functions in `src/python.rs`),
  behind the existing build.
- Native code that runs outside a Python process (the stream and service engine) stays in
  `mage-service`; code Rust blocks use stays in the `mage` crate.
- If the extension is ever published for use outside MageML, it is renamed then.
