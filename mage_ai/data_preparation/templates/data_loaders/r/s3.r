suppressPackageStartupMessages(library(tidyverse))

# Reads a file from S3 in the format its extension tells. The credentials come
# from the AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION settings of
# the "default" profile in io_config.yaml; set AWS_ENDPOINT for S3-compatible
# storage such as MinIO. Without them, arrow uses the AWS environment and
# ~/.aws.
#* @data_loader
load_data <- function(...) {
  read_s3("s3://your-bucket/path/input.parquet", profile = "default")
}

#* @test
output_has_rows <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
