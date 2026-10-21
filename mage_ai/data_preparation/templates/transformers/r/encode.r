suppressPackageStartupMessages(library(tidyverse))

# One-hot encoding: a column per value of the column the encode variable names
# (category by default), 1 where the row has the value and 0 elsewhere. The
# new columns replace the encoded one.
#* @transformer
transform <- function(df_1, ...) {
  column <- variable("encode", default = "category")
  df_1 |>
    mutate(.row = row_number(), .one = 1L) |>
    pivot_wider(
      names_from = all_of(column),
      values_from = .one,
      names_prefix = str_c(column, "_"),
      values_fill = 0L
    ) |>
    select(-.row)
}
