suppressPackageStartupMessages(library(tidyverse))

# Writes df_1 in the format the extension tells: CSV, TSV, Parquet,
# Feather/Arrow, JSON, NDJSON or RDS, creating the directory. Relative paths
# start at the Mage project.
#* @data_exporter
export_data <- function(df_1, ...) {
  write_file(df_1, "output/result.parquet")
}
