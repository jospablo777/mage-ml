suppressPackageStartupMessages(library(tidyverse))

# Turns columns into rows with tidyr: one row per id and column. Replace id
# with the columns that identify a row; pivot_wider() does the reverse.
#* @transformer
transform <- function(df_1, ...) {
  df_1 |>
    pivot_longer(
      cols = -id,
      names_to = "variable",
      values_to = "value",
      values_transform = as.character
    )
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
