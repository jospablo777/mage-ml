#' The format of a data file, from its extension
#'
#' @param path A file path or S3 URI.
#' @param format A format to use instead of the extension's.
#' @return One of `"csv"`, `"tsv"`, `"parquet"`, `"feather"`, `"json"`,
#'   `"ndjson"` or `"rds"`.
#' @noRd
file_format <- function(path, format = NULL) {
  if (!is.null(format)) {
    format <- tolower(format)
  } else {
    extension <- tolower(tools::file_ext(sub("\\.(gz|bz2|xz|zst)$", "", path)))
    format <- switch(extension,
      csv = "csv",
      tsv = ,
      tab = "tsv",
      parquet = ,
      pq = "parquet",
      feather = ,
      arrow = ,
      ipc = "feather",
      json = "json",
      jsonl = ,
      ndjson = "ndjson",
      rds = "rds",
      extension
    )
  }
  formats <- c("csv", "tsv", "parquet", "feather", "json", "ndjson", "rds")
  if (!format %in% formats) {
    stop(
      sprintf(
        "Cannot tell the format of %s; set format to one of %s.",
        path,
        paste(formats, collapse = ", ")
      ),
      call. = FALSE
    )
  }
  format
}

#' Read a data file into a tibble
#'
#' Reads a local file, with the format taken from its extension: CSV and TSV
#' with readr, Parquet, Feather/Arrow and NDJSON with arrow, JSON arrays with
#' jsonlite, and RDS files. Relative paths start at the Mage project, which
#' is the working directory of R blocks.
#'
#' @param path The path of the file.
#' @param format `"csv"`, `"tsv"`, `"parquet"`, `"feather"`, `"json"`,
#'   `"ndjson"` or `"rds"`, when the extension does not tell it.
#' @param ... Other arguments of the reading function, such as
#'   `col_types` for CSV files.
#' @return A tibble.
#' @export
#' @examples
#' \dontrun{
#' read_file("data/orders.csv")
#' read_file("data/orders.parquet")
#' }
read_file <- function(path, format = NULL, ...) {
  format <- file_format(path, format)
  data <- switch(format,
    csv = {
      require_packages("readr")
      readr::read_csv(path, show_col_types = FALSE, ...)
    },
    tsv = {
      require_packages("readr")
      readr::read_tsv(path, show_col_types = FALSE, ...)
    },
    parquet = arrow::read_parquet(path, ...),
    feather = arrow::read_feather(path, ...),
    ndjson = arrow::read_json_arrow(path, ...),
    json = jsonlite::fromJSON(path, ...),
    rds = readRDS(path)
  )
  tibble::as_tibble(data)
}

#' Write a data frame to a file
#'
#' Writes a local file in the format its extension tells, creating its
#' directory. CSV and TSV are written with readr, Parquet, Feather/Arrow and
#' NDJSON with arrow, JSON arrays with jsonlite, and RDS files with
#' [saveRDS()].
#'
#' @param df A data frame.
#' @param path The path of the file.
#' @param format The format, when the extension does not tell it.
#' @param ... Other arguments of the writing function.
#' @return `path`, invisibly.
#' @export
#' @examples
#' \dontrun{
#' write_file(df, "output/orders.parquet")
#' }
write_file <- function(df, path, format = NULL, ...) {
  format <- file_format(path, format)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  switch(format,
    csv = {
      require_packages("readr")
      readr::write_csv(df, path, ...)
    },
    tsv = {
      require_packages("readr")
      # write_tsv doubles quotes without quoting the field, which read_tsv
      # does not undo; quoting such fields makes the round trip exact.
      do.call(
        readr::write_tsv,
        c(list(df, path), utils::modifyList(list(quote = "needed"), list(...)))
      )
    },
    parquet = arrow::write_parquet(df, path, ...),
    feather = arrow::write_feather(df, path, ...),
    ndjson = {
      connection <- file(path, open = "w", encoding = "UTF-8")
      on.exit(close(connection), add = TRUE)
      do.call(
        jsonlite::stream_out,
        c(list(df, connection, verbose = FALSE), json_options(...))
      )
    },
    json = do.call(jsonlite::write_json, c(list(df, path), json_options(...))),
    rds = saveRDS(df, path, ...)
  )
  invisible(path)
}

#' The options of jsonlite when writing data frames
#'
#' jsonlite writes numbers with 4 significant digits by default, which rounds
#' values; all digits are kept, missing values are null and times are ISO
#' 8601. Options in `...` replace these.
#'
#' @noRd
json_options <- function(...) {
  utils::modifyList(
    list(digits = NA, na = "null", POSIXt = "ISO8601", dataframe = "rows"),
    list(...)
  )
}
