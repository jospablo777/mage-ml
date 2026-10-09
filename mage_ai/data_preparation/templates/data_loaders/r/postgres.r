suppressPackageStartupMessages(library(tidyverse))

# Connects with the settings of the "default" profile in io_config.yaml, with
# DBI and RPostgres, which `mage r init` installs. In an R environment of your
# own, add them with `rv add DBI RPostgres`.
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
