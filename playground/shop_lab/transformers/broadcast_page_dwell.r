library(dplyr)

#* @transformer
page_dwell <- function(events, ...) {
  # How long visitors stay on each page, in R: median and 90th percentile.
  events |>
    filter(!is.na(duration_ms)) |>
    group_by(page) |>
    summarise(
      views = n(),
      median_ms = median(duration_ms),
      p90_ms = quantile(duration_ms, 0.9, names = FALSE),
      .groups = "drop"
    ) |>
    arrange(desc(views))
}
