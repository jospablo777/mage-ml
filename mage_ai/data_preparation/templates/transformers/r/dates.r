suppressPackageStartupMessages(library(tidyverse))

# Adds features of a date or date-time column, the one the date_column variable
# names (created_at by default): its day, its ISO week day (1 is Monday), its
# month and hour, and whether the day falls on a weekend.
#* @transformer
transform <- function(df_1, ...) {
  column <- variable("date_column", default = "created_at")
  at <- as_datetime(df_1[[column]])
  df_1 |>
    mutate(
      day = as_date(at),
      weekday = wday(at, week_start = 1),
      month = month(at),
      hour = hour(at),
      weekend = weekday >= 6
    )
}
