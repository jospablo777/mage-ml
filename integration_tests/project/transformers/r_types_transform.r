suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(pointblank))

#* @transformer
add_columns <- function(df_1, ...) {
  df_1 |>
    mutate(
      r_big_plus_one = c_big + 1L,
      r_text_length = str_length(c_text),
      r_list_length = map_int(c_int_list, length),
      r_date_year = year(c_date),
      r_ts_plus_hour = c_ts + hours(1),
      r_duration_seconds = as.numeric(c_duration, units = "secs"),
      r_struct_a = map_dbl(c_struct, \(x) if (is.null(x$a)) NA_real_ else x$a),
      r_scaled = c_double * variable("multiplier"),
      r_label = str_c(variable("prefix"), coalesce(c_text, ""))
    )
}

#* @test
r_types <- function(output) {
  stopifnot(
    is.integer(output$c_int),
    bit64::is.integer64(output$c_big),
    bit64::is.integer64(output$r_big_plus_one),
    is.factor(output$c_category),
    inherits(output$c_date, "Date"),
    inherits(output$c_ts, "POSIXct"),
    identical(attr(output$c_tstz, "tzone"), "America/New_York"),
    inherits(output$c_duration, "difftime"),
    inherits(output$c_time, "hms"),
    is.list(output$c_struct)
  )
}

# pointblank's validation functions stop at the first step that fails.
#* @test
pointblank_checks <- function(output) {
  output |>
    col_vals_not_null(id) |>
    rows_distinct(id) |>
    col_vals_in_set(c_category, c("low", "mid", "high", NA)) |>
    col_vals_gte(r_list_length, 0)
}
