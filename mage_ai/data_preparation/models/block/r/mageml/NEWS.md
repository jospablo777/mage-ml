# mageml 0.1.0

* First release, with MageML 0.1.0. Runs R data loaders, transformers, data
  exporters, custom blocks and conditionals (`#* @condition`, `condition()`),
  exchanges data frames with Python and Rust blocks through Arrow IPC, and
  connects to the databases of a project's `io_config.yaml` and to S3.
* `serve_stream()` runs a streaming pipeline's R transformers in one R session
  that stays up for the stream: each batch of messages arrives as a tibble,
  one row per JSON message, with integers past 2^53 kept as integer64.
