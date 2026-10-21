suppressPackageStartupMessages(library(tidyverse))

# Keeps the latest row of each key. The key column comes from the key variable
# (id by default), and the column whose largest value is the latest from
# order_by (updated_at by default). Rows without a key are dropped.
#* @transformer
transform <- function(df_1, ...) {
  key <- variable("key", default = "id")
  order_by <- variable("order_by", default = "updated_at")
  df_1 |>
    filter(!is.na(.data[[key]])) |>
    slice_max(.data[[order_by]], n = 1, by = all_of(key), with_ties = FALSE)
}

#* @test
keys_are_unique <- function(output) {
  key <- variable("key", default = "id")
  stopifnot("A key repeats" = !anyDuplicated(output[[key]]))
}
