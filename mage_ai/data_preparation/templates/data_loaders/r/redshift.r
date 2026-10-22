suppressPackageStartupMessages(library(tidyverse))

# Connects with the settings of the "default" profile in io_config.yaml
# (REDSHIFT_HOST, REDSHIFT_DBNAME, REDSHIFT_TEMP_CRED_USER and
# REDSHIFT_TEMP_CRED_PASSWORD; REDSHIFT_SCHEMA as the search path), with DBI and
# RPostgres, which `mage r init` installs. In an R environment of your own, add
# them with `rv add DBI RPostgres`.
#* @data_loader
load_data <- function(...) {
  read_sql(
    "SELECT * FROM your_table WHERE updated_at >= $1",
    database = "redshift",
    profile = "default",
    params = list(as.Date(variable("execution_date")))
  )
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
