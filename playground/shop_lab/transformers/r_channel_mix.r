library(dplyr)
library(tidyr)
library(tibble)

#* @transformer
channel_mix <- function(orders, ...) {
  # Orders per segment and channel, one column per channel (tidyr::pivot_wider).
  orders |>
    count(segment, channel) |>
    group_by(segment) |>
    mutate(share = round(n / sum(n), 4)) |>
    ungroup() |>
    select(-n) |>
    pivot_wider(names_from = channel, values_from = share, values_fill = 0) |>
    as_tibble()
}
