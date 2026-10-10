library(dplyr)
library(tibble)

#* @transformer
churn_scores <- function(activity, ...) {
  # The pandas frame arrives as a tibble. RFM scores from 1 to 5 with dplyr::ntile,
  # and a churn risk from the scores.
  activity |>
    mutate(
      recency_score = 6L - ntile(days_since_last_order, 5),
      frequency_score = ntile(orders, 5),
      spend_score = ntile(spend, 5),
      rfm = recency_score * 100L + frequency_score * 10L + spend_score,
      churn_risk = case_when(
        recency_score <= 2 & frequency_score >= 3 ~ "at risk",
        recency_score <= 2 ~ "lapsed",
        recency_score >= 4 & frequency_score >= 4 ~ "loyal",
        TRUE ~ "steady"
      ),
      churn_risk = factor(churn_risk, levels = c("loyal", "steady", "at risk", "lapsed"))
    ) |>
    arrange(desc(rfm)) |>
    as_tibble()
}

#* @test
scores_are_in_range <- function(output) {
  stopifnot(
    "A score is outside 1 to 5" = all(output$recency_score %in% 1:5),
    "A churn risk is missing" = !anyNA(output$churn_risk)
  )
}
