suppressPackageStartupMessages(library(tidyverse))

# Joins the outputs of two upstream blocks: df_1 is the first, df_2 the
# second. Replace id with the columns that match their rows.
#* @transformer
transform <- function(df_1, df_2, ...) {
  df_1 |>
    left_join(df_2, by = join_by(id))
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
