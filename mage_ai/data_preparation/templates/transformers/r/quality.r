suppressPackageStartupMessages(library(tidyverse))

# Checks the rows before they go on and stops with every problem found: the
# columns of the required variable (id by default) have no missing values, the
# column of the unique variable (id by default) has no repeated values, and
# amount, when the data has it, is not negative. The rows pass on unchanged.
#* @transformer
transform <- function(df_1, ...) {
  required <- unlist(variable("required", default = list("id")))
  unique_key <- variable("unique", default = "id")
  problems <- character()
  for (column in required) {
    missing <- sum(is.na(df_1[[column]]))
    if (missing > 0) {
      problems <- c(problems, str_glue("{column} is missing in {missing} rows"))
    }
  }
  repeated <- sum(duplicated(df_1[[unique_key]], incomparables = NA))
  if (repeated > 0) {
    problems <- c(problems, str_glue("{unique_key} repeats in {repeated} rows"))
  }
  if ("amount" %in% names(df_1)) {
    negative <- sum(df_1$amount < 0, na.rm = TRUE)
    if (negative > 0) {
      problems <- c(problems, str_glue("amount is negative in {negative} rows"))
    }
  }
  if (length(problems) > 0) {
    stop(
      "The rows failed their checks: ", str_flatten(problems, "; "),
      call. = FALSE
    )
  }
  df_1
}
