suppressPackageStartupMessages({
  library(tidyverse)
  library(httr2)
})

# Requests JSON from an API with httr2, which `mage r init` installs. Failed
# requests are retried, and an error status fails the block. For a token, add
# `req_auth_bearer_token(Sys.getenv("API_TOKEN"))` to the request.
#* @data_loader
load_data <- function(...) {
  request("https://api.example.com/v1/orders") |>
    req_url_query(since = variable("execution_date")) |>
    req_headers(Accept = "application/json") |>
    req_retry(max_tries = 3) |>
    req_perform() |>
    # Integers above 2^53 come as text; as numbers they would be rounded.
    resp_body_json(simplifyVector = TRUE, bigint_as_char = TRUE) |>
    as_tibble()
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
