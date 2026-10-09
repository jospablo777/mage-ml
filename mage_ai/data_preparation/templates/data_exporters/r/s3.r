suppressPackageStartupMessages(library(tidyverse))

# Writes df_1 to S3 in the format the extension tells. The credentials come
# from the AWS_ settings of the "default" profile in io_config.yaml; set
# AWS_ENDPOINT for S3-compatible storage such as MinIO.
#* @data_exporter
export_data <- function(df_1, ...) {
  write_s3(df_1, "s3://your-bucket/path/result.parquet", profile = "default")
}
