library(dplyr)
library(lubridate)
library(tibble)

#* @transformer
cohort_retention <- function(orders, ...) {
  # Customers grouped by the month they signed up; the share of each cohort that
  # ordered 0, 1, 2, ... months after signing up.
  activity <- orders |>
    mutate(
      cohort = floor_date(as_date(signed_up_at), "month"),
      month = floor_date(as_date(ordered_at), "month"),
      months_since_signup = as.integer(interval(cohort, month) %/% months(1))
    ) |>
    filter(months_since_signup >= 0) |>
    distinct(customer_id, cohort, months_since_signup)

  cohort_sizes <- activity |>
    distinct(customer_id, cohort) |>
    count(cohort, name = "customers")

  activity |>
    count(cohort, months_since_signup, name = "active_customers") |>
    left_join(cohort_sizes, by = "cohort") |>
    mutate(retention = round(active_customers / customers, 4)) |>
    arrange(cohort, months_since_signup) |>
    as_tibble()
}

#* @test
retention_is_a_share <- function(output) {
  stopifnot(
    "Retention outside 0 to 1" = all(output$retention >= 0 & output$retention <= 1)
  )
}
