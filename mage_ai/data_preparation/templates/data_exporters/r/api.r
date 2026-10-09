suppressPackageStartupMessages({
  library(tidyverse)
  library(httr2)
})

# Sends df_1 to an API as JSON, in batches of rows, with httr2. An error
# status fails the block. For a token, add
# `req_auth_bearer_token(Sys.getenv("API_TOKEN"))` to the request.
#* @data_exporter
export_data <- function(df_1, ...) {
  batches <- split(df_1, ceiling(seq_len(nrow(df_1)) / 500))
  for (batch in batches) {
    request("https://api.example.com/v1/orders") |>
      req_body_json(batch, dataframe = "rows", na = "null") |>
      req_retry(max_tries = 3) |>
      req_perform()
  }
}
