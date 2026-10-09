#' An S3 file system with the settings of an io_config.yaml profile
#'
#' Creates an arrow S3 file system with the `AWS_ACCESS_KEY_ID`,
#' `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION` and
#' `AWS_ENDPOINT` settings of a profile in the Mage project's
#' `io_config.yaml`. `AWS_ENDPOINT` points to S3-compatible storage, such as
#' MinIO. Without a profile or its settings, arrow finds the credentials as
#' the AWS tools do, from the environment and `~/.aws`.
#'
#' @param profile The name of the profile. Write it as text in the block, so
#'   that Mage passes its settings.
#' @return An arrow `S3FileSystem`.
#' @export
#' @examples
#' \dontrun{
#' fs <- s3_filesystem("default")
#' arrow::read_parquet(fs$path("bucket/orders.parquet"))
#' }
s3_filesystem <- function(profile = "default") {
  settings <- state$io_config[[profile]]
  if (is.null(settings)) {
    settings <- list()
  }
  arguments <- list(
    access_key = settings$AWS_ACCESS_KEY_ID,
    secret_key = settings$AWS_SECRET_ACCESS_KEY,
    session_token = settings$AWS_SESSION_TOKEN,
    region = settings$AWS_REGION
  )
  endpoint <- settings$AWS_ENDPOINT
  if (!is.null(endpoint) && nzchar(endpoint)) {
    parsed <- parse_endpoint(endpoint)
    arguments$endpoint_override <- parsed$endpoint
    arguments$scheme <- parsed$scheme
  }
  arguments <- arguments[!vapply(arguments, is_blank, logical(1))]
  do.call(arrow::S3FileSystem$create, arguments)
}

is_blank <- function(value) {
  is.null(value) || identical(value, "")
}

#' The scheme and host of an endpoint URL, as arrow takes them
#'
#' @param endpoint Such as `http://localhost:9000` or `minio:9000`.
#' @noRd
parse_endpoint <- function(endpoint) {
  scheme <- "https"
  match <- regmatches(endpoint, regexec("^(https?)://(.*)$", endpoint))[[1]]
  if (length(match) == 3) {
    scheme <- match[[2]]
    endpoint <- match[[3]]
  }
  list(scheme = scheme, endpoint = sub("/+$", "", endpoint))
}

#' The bucket and key of an S3 URI
#'
#' @param uri Such as `s3://bucket/path/file.parquet`.
#' @noRd
parse_s3_uri <- function(uri) {
  match <- regmatches(uri, regexec("^s3://([^/]+)/(.+)$", uri))[[1]]
  if (length(match) != 3) {
    stop(
      sprintf("%s is not an S3 URI such as s3://bucket/key.", uri),
      call. = FALSE
    )
  }
  list(bucket = match[[2]], key = match[[3]])
}

#' A temporary file with the extensions of a path
#' @noRd
temporary_file <- function(path) {
  tempfile(fileext = sub("^[^.]*", "", basename(path)))
}

#' Read a data file from S3 into a tibble
#'
#' Reads a file of an S3 bucket, in the format its extension tells, as
#' [read_file()] reads local files.
#'
#' @param uri The file, such as `s3://bucket/path/orders.parquet`.
#' @param profile The io_config.yaml profile with the S3 settings; see
#'   [s3_filesystem()].
#' @param format The format, when the extension does not tell it.
#' @param ... Other arguments of the reading function.
#' @return A tibble.
#' @export
#' @examples
#' \dontrun{
#' read_s3("s3://bucket/orders.parquet", profile = "default")
#' }
read_s3 <- function(uri, profile = "default", format = NULL, ...) {
  format <- file_format(uri, format)
  parts <- parse_s3_uri(uri)
  fs <- s3_filesystem(profile)
  local <- temporary_file(parts$key)
  on.exit(unlink(local), add = TRUE)
  input <- fs$OpenInputFile(paste(parts$bucket, parts$key, sep = "/"))
  data <- tryCatch(input$Read(), finally = input$close())
  writeBin(as.raw(data), local)
  read_file(local, format = format, ...)
}

#' Write a data frame to S3
#'
#' Writes a file to an S3 bucket in the format its extension tells, as
#' [write_file()] writes local files.
#'
#' @param df A data frame.
#' @param uri The file, such as `s3://bucket/path/orders.parquet`.
#' @param profile The io_config.yaml profile with the S3 settings; see
#'   [s3_filesystem()].
#' @param format The format, when the extension does not tell it.
#' @param ... Other arguments of the writing function.
#' @return `uri`, invisibly.
#' @export
#' @examples
#' \dontrun{
#' write_s3(df, "s3://bucket/orders.parquet", profile = "default")
#' }
write_s3 <- function(df, uri, profile = "default", format = NULL, ...) {
  format <- file_format(uri, format)
  parts <- parse_s3_uri(uri)
  fs <- s3_filesystem(profile)
  local <- temporary_file(parts$key)
  on.exit(unlink(local), add = TRUE)
  write_file(df, local, format = format, ...)
  output <- fs$OpenOutputStream(paste(parts$bucket, parts$key, sep = "/"))
  tryCatch(
    output$write(readBin(local, "raw", file.size(local))),
    finally = output$close()
  )
  invisible(uri)
}
