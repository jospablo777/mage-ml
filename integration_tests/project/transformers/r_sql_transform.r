suppressPackageStartupMessages(library(tidyverse))

#* @transformer
summarise_rows <- function(df_1, ...) {
  df_1 |>
    mutate(
      r_text_length = str_length(c_text),
      r_int_array_length = map_int(c_int_array, length),
      r_jsonb_keys = map_int(c_jsonb, \(x) if (is.list(x)) length(x) else NA_integer_),
      r_day = as.character(c_date)
    )
}

#* @test
ids_are_unique <- function(output) {
  stopifnot(!anyDuplicated(output$id))
}
