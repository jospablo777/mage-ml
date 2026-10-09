suppressPackageStartupMessages(library(tidyverse))

# Connects with the settings of the "default" profile in io_config.yaml, with
# DBI and duckdb, which `mage r init` installs. In an R environment of your
# own, add them with `rv add DBI duckdb`.
#* @data_loader
load_data <- function(...) {
  read_sql(
    "SELECT * FROM your_table WHERE updated_at >= ?",
    database = "duckdb",
    profile = "default",
    params = list(as.Date(variable("execution_date")))
  )
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
