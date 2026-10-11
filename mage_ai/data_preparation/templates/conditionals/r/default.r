# Decides whether the block this condition is attached to runs: return TRUE to run it,
# FALSE to skip it. It receives the same upstream outputs as that block (df_1, df_2, ...)
# and reads the pipeline's variables with variable().
#* @condition
should_run <- function(...) {
  isTRUE(variable("enabled", TRUE))
}
