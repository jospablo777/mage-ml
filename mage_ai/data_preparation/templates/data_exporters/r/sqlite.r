suppressPackageStartupMessages(library(tidyverse))

# Writes df_1 to a table of a SQLite database file with DBI and RSQLite, and
# creates the file and its directory. Relative paths start at the Mage project.
#* @data_exporter
export_data <- function(df_1, ...) {
  path <- "data/database.sqlite"
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  connection <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(connection))
  DBI::dbWriteTable(connection, "your_table", df_1, overwrite = TRUE)
}
