suppressPackageStartupMessages(library(tidyverse))

# Connects with the settings of the "default" profile in io_config.yaml. Needs
# the RMariaDB package: run `rv add RMariaDB` in the project's R environment.
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
