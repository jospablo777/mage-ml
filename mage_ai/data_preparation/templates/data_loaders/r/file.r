suppressPackageStartupMessages(library(tidyverse))

# Reads a file in the format its extension tells: CSV, TSV, Parquet,
# Feather/Arrow, JSON, NDJSON or RDS. Relative paths start at the Mage
# project. read_file() passes other arguments to the reader, such as
# col_types for CSV files.
#* @data_loader
load_data <- function(...) {
  read_file("data/input.csv")
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
