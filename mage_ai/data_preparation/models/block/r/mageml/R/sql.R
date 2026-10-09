#' Run a query on a database of io_config.yaml
#'
#' Connects with [db_connect()], runs the query and disconnects.
#'
#' @inheritParams db_connect
#' @param query The SQL query.
#' @param params Values for the query's placeholders, such as `$1` in
#'   Postgres or `?` in MySQL and DuckDB, as in [DBI::dbGetQuery()].
#' @return A tibble.
#' @export
#' @examples
#' \dontrun{
#' data_loader(function() {
#'   read_sql(
#'     "SELECT * FROM orders WHERE day = $1",
#'     params = list(as.Date(variable("execution_date")))
#'   )
#' })
#' }
read_sql <- function(query,
                     database = c("postgres", "mysql", "duckdb"),
                     profile = "default",
                     params = NULL) {
  database <- match.arg(database)
  connection <- db_connect(database, profile = profile)
  on.exit(DBI::dbDisconnect(connection))
  result <- if (is.null(params)) {
    DBI::dbGetQuery(connection, query)
  } else {
    DBI::dbGetQuery(connection, query, params = params)
  }
  tibble::as_tibble(result)
}

#' Format dates as ISO 8601 text
#'
#' R formats the year 1 as `"1"`, which databases read as 2001.
#'
#' @param x A `Date` vector.
#' @return A character vector, with `NA` for `NA`.
#' @noRd
sql_date <- function(x) {
  parts <- as.POSIXlt(x, tz = "UTC")
  text <- sprintf(
    "%04d-%02d-%02d",
    parts$year + 1900L,
    parts$mon + 1L,
    parts$mday
  )
  text[is.na(x)] <- NA_character_
  text
}

#' Format date-times as ISO 8601 text with microseconds
#'
#' RPostgres truncates microseconds, so a date-time held as a double just
#' below its microsecond lost it, and R formats years before 1000 with fewer
#' than four digits. Zoned date-times are written in UTC with their offset.
#'
#' @param x A `POSIXct` vector.
#' @param zone Whether to append the UTC offset.
#' @return A character vector, with `NA` for `NA`.
#' @noRd
sql_datetime <- function(x, zone) {
  micros <- datetime_microseconds(x)
  fraction <- as.integer(micros %% 1000000L)
  seconds <- as.numeric((micros - fraction) %/% 1000000L)
  parts <- as.POSIXlt(seconds, origin = "1970-01-01", tz = "UTC")
  text <- sprintf(
    "%04d-%02d-%02d %02d:%02d:%02d.%06d%s",
    parts$year + 1900L,
    parts$mon + 1L,
    parts$mday,
    parts$hour,
    parts$min,
    as.integer(parts$sec),
    fraction,
    if (zone) "+00:00" else ""
  )
  text[is.na(x)] <- NA_character_
  text
}

#' Prepare a data frame for DBI::dbWriteTable()
#'
#' Dates and date-times become ISO 8601 text with the column types that
#' read it, list columns become JSON text, and Arrow binary columns become
#' blobs.
#'
#' @param df A data frame.
#' @param database `"postgres"`, `"mysql"` or `"duckdb"`.
#' @return A list with the data frame and its `field_types`.
#' @noRd
sql_values <- function(df, database) {
  types <- list(
    postgres = c(date = "date", naive = "timestamp", zoned = "timestamptz"),
    mysql = c(date = "DATE", naive = "DATETIME(6)", zoned = "DATETIME(6)"),
    duckdb = c(date = "DATE", naive = "TIMESTAMP", zoned = "TIMESTAMPTZ")
  )[[database]]
  df <- as.data.frame(df, stringsAsFactors = FALSE, optional = TRUE)
  field_types <- character(0)
  for (column in names(df)) {
    x <- df[[column]]
    if (inherits(x, "Date")) {
      df[[column]] <- sql_date(x)
      field_types[[column]] <- types[["date"]]
    } else if (inherits(x, "POSIXt")) {
      x <- as.POSIXct(x)
      # MySQL's DATETIME has no time zone; zoned date-times are stored in UTC.
      zone <- !is_naive(x) && database != "mysql"
      df[[column]] <- sql_datetime(x, zone)
      field_types[[column]] <- types[[if (is_naive(x)) "naive" else "zoned"]]
    } else if (inherits(x, "arrow_binary")) {
      # as.raw(NULL) is raw(0), which would write an empty value for NULL.
      df[[column]] <- blob::new_blob(
        lapply(x, function(value) if (is.null(value)) NULL else as.raw(value))
      )
    } else if (is.list(x) && !inherits(x, "blob")) {
      df[[column]] <- to_json_text(x)
    }
  }
  list(df = df, field_types = field_types)
}

#' Write a data frame to a table of a database of io_config.yaml
#'
#' Connects with [db_connect()], writes the data frame with
#' [DBI::dbWriteTable()] and disconnects. List columns are written as JSON
#' text. Dates and date-times are written as ISO 8601 text that Mage formats,
#' to keep every microsecond and the years before 1000.
#'
#' @inheritParams db_connect
#' @param df A data frame.
#' @param table The name of the table.
#' @param schema The schema of the table. Defaults to the connection's: the
#'   profile's `POSTGRES_SCHEMA` or `DUCKDB_SCHEMA`.
#' @param if_exists What to do when the table exists: `"replace"` it,
#'   `"append"` to it or `"fail"`.
#' @return The number of rows written, invisibly.
#' @export
#' @examples
#' \dontrun{
#' #* @data_exporter
#' export_orders <- function(orders, ...) {
#'   write_table(orders, "orders", if_exists = "append")
#' }
#' }
write_table <- function(df,
                        table,
                        database = c("postgres", "mysql", "duckdb"),
                        profile = "default",
                        schema = NULL,
                        if_exists = c("replace", "append", "fail")) {
  database <- match.arg(database)
  if_exists <- match.arg(if_exists)
  require_packages("blob")
  prepared <- sql_values(df, database)
  connection <- db_connect(database, profile = profile)
  on.exit(DBI::dbDisconnect(connection))
  name <- if (is.null(schema)) {
    DBI::Id(table = table)
  } else {
    DBI::Id(schema = schema, table = table)
  }
  exists <- DBI::dbExistsTable(connection, name)
  if (if_exists == "fail" && exists) {
    stop(sprintf("The table %s exists.", table), call. = FALSE)
  }
  append <- if_exists == "append" && exists
  # Column types apply to the tables dbWriteTable() creates.
  field_types <- if (append || length(prepared$field_types) == 0) {
    NULL
  } else {
    prepared$field_types
  }
  DBI::dbWriteTable(
    connection,
    name,
    prepared$df,
    overwrite = if_exists == "replace",
    append = append,
    field.types = field_types
  )
  invisible(nrow(prepared$df))
}
