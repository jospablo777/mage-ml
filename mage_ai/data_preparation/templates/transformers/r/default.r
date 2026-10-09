suppressPackageStartupMessages(library(tidyverse))

#* @transformer
transform <- function(df_1, ...) {
  # df_1 is the output of the first upstream block, as a tibble; df_2 of the
  # second. Return the transformed data frame.
  df_1 |>
    mutate()
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
