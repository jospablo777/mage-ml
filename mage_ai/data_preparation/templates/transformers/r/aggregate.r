suppressPackageStartupMessages(library(tidyverse))

# Groups the rows and sums the numeric columns of each group. Replace
# your_column with the columns to group by.
#* @transformer
transform <- function(df_1, ...) {
  df_1 |>
    group_by(your_column) |>
    summarise(
      rows = n(),
      across(where(is.numeric), \(value) sum(value, na.rm = TRUE)),
      .groups = "drop"
    )
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
