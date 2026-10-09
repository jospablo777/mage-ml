suppressPackageStartupMessages(library(tidyverse))

# Connects with the settings of the "default" profile in io_config.yaml, with
# DBI and RMariaDB, which `mage r init` installs. In an R environment of your
# own, add them with `rv add DBI RMariaDB`.
#* @data_exporter
export_data <- function(df_1, ...) {
  write_table(
    df_1,
    "your_table",
    database = "mysql",
    profile = "default",
    if_exists = "replace"
  )
}
