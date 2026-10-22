suppressPackageStartupMessages(library(tidyverse))

# Connects with the settings of the "default" profile in io_config.yaml
# (REDSHIFT_HOST, REDSHIFT_DBNAME, REDSHIFT_TEMP_CRED_USER and
# REDSHIFT_TEMP_CRED_PASSWORD; REDSHIFT_SCHEMA as the search path), with DBI and
# RPostgres, which `mage r init` installs. In an R environment of your own, add
# them with `rv add DBI RPostgres`. Rows are inserted in batches; for millions
# of rows, write a file to S3 and load it with Redshift's COPY.
#* @data_exporter
export_data <- function(df_1, ...) {
  write_table(
    df_1,
    "your_table",
    database = "redshift",
    profile = "default",
    if_exists = "replace"
  )
}
