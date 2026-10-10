#* @custom
custom <- function(...) {
  # A custom block takes the outputs of its upstream blocks, if any, in their
  # order, and returns anything: a data frame, a list, a value or nothing.
  # context() names the block and pipeline; variable("name") reads a pipeline
  # variable.
  inputs <- list(...)
  list(
    block = context()$block_uuid,
    inputs = length(inputs)
  )
}
