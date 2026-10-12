library(dplyr)

#* @transformer
store_margin <- function(lines, ...) {
  # The other branch of the Diamond, in R: cost and gross margin per store.
  lines |>
    group_by(store_id) |>
    summarise(
      cost = sum(quantity * cost),
      gross = sum(quantity * unit_price),
      .groups = "drop"
    ) |>
    mutate(margin = gross - cost) |>
    select(store_id, cost, margin) |>
    arrange(store_id)
}
