suppressPackageStartupMessages(library(tidyverse))

# Connects with the settings of the "default" profile in io_config.yaml. Needs
# the RPostgres package: run `rv add RPostgres` in the project's R environment.
#* @data_loader
load_data <- function(...) {
  read_sql(
    "SELECT * FROM your_table WHERE updated_at >= $1",
    database = "postgres",
    profile = "default",
    params = list(as.Date(variable("execution_date")))
  )
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
