suppressPackageStartupMessages(library(tidyverse))

#* @transformer
add_columns <- function(df_1, ...) {
  df_1 |>
    mutate(
      text_length = str_length(c_text),
      bigint_plus_one = c_bigint + 1L,
      date_year = as.integer(year(c_date))
    )
}
