suppressPackageStartupMessages(library(tidyverse))

# Cleans a data frame: snake_case column names, trimmed text, no empty rows
# and no duplicate rows.
#* @transformer
transform <- function(df_1, ...) {
  df_1 |>
    rename_with(\(name) {
      name |>
        str_to_lower() |>
        str_replace_all("[^a-z0-9]+", "_") |>
        str_remove_all("^_|_$")
    }) |>
    mutate(across(where(is.character), str_squish)) |>
    filter(if_any(everything(), \(value) !is.na(value))) |>
    distinct()
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
