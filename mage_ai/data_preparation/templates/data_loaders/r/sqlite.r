suppressPackageStartupMessages(library(tidyverse))

# Reads a SQLite database file with DBI and RSQLite, which `mage r init`
# installs. Relative paths start at the Mage project.
#* @data_loader
load_data <- function(...) {
  connection <- DBI::dbConnect(RSQLite::SQLite(), "data/database.sqlite")
  on.exit(DBI::dbDisconnect(connection))
  DBI::dbGetQuery(connection, "SELECT * FROM your_table") |>
    as_tibble()
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
