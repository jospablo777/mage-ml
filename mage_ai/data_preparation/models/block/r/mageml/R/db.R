#' Database settings of an io_config.yaml profile
#'
#' Returns the database settings of a profile in the Mage project's
#' `io_config.yaml`, such as `POSTGRES_HOST`, with environment variables and
#' secrets resolved. Mage passes the profiles whose names appear as text in
#' the block's code, and `"default"`; settings other than those of Postgres,
#' MySQL and DuckDB are left out.
#'
#' @param profile The name of the profile.
#' @return A named list of settings.
#' @export
#' @examples
#' \dontrun{
#' io_config("default")$POSTGRES_HOST
#' }
io_config <- function(profile = "default") {
  profiles <- state$io_config
  if (is.null(profiles[[profile]])) {
    stop(
      sprintf(
        paste0(
          "io_config.yaml has no profile %s with database settings. Mage ",
          "passes the profiles whose names appear as text in the block, ",
          "such as db_connect(\"postgres\", profile = \"%s\")."
        ),
        profile,
        profile
      ),
      call. = FALSE
    )
  }
  profiles[[profile]]
}

check_direct <- function(method, database) {
  if (!is.null(method) && !identical(tolower(method), "direct")) {
    stop(
      sprintf("db_connect() supports direct %s connections only.", database),
      call. = FALSE
    )
  }
}

#' The arguments of DBI::dbConnect() for a database
#'
#' @param database `"postgres"`, `"mysql"` or `"duckdb"`.
#' @param config The settings of an io_config.yaml profile.
#' @return A named list without the settings the profile lacks.
#' @noRd
db_arguments <- function(database, config) {
  setting <- function(key) {
    value <- config[[key]]
    if (is.null(value) || identical(value, "")) NULL else value
  }
  arguments <- switch(database,
    postgres = {
      check_direct(setting("POSTGRES_CONNECTION_METHOD"), "Postgres")
      schema <- setting("POSTGRES_SCHEMA")
      list(
        dbname = setting("POSTGRES_DBNAME"),
        host = setting("POSTGRES_HOST"),
        port = setting("POSTGRES_PORT"),
        user = setting("POSTGRES_USER"),
        password = setting("POSTGRES_PASSWORD"),
        connect_timeout = setting("POSTGRES_CONNECT_TIMEOUT"),
        options = if (!is.null(schema)) paste0("-c search_path=", schema)
      )
    },
    mysql = {
      check_direct(setting("MYSQL_CONNECTION_METHOD"), "MySQL")
      port <- setting("MYSQL_PORT")
      list(
        dbname = setting("MYSQL_DATABASE"),
        host = setting("MYSQL_HOST"),
        port = if (!is.null(port)) as.integer(port),
        user = setting("MYSQL_USER"),
        password = setting("MYSQL_PASSWORD")
      )
    },
    duckdb = list(dbdir = setting("DUCKDB_DATABASE"))
  )
  arguments[!vapply(arguments, is.null, logical(1))]
}

#' Connect to a database of io_config.yaml
#'
#' Opens a DBI connection with the settings of a profile in the Mage
#' project's `io_config.yaml`. Postgres needs the RPostgres package, MySQL
#' RMariaDB and DuckDB duckdb; add them to the project's R environment with
#' `rv add`. Close the connection with [DBI::dbDisconnect()].
#'
#' A Postgres connection uses `POSTGRES_SCHEMA` as its search path, and a
#' DuckDB connection `DUCKDB_SCHEMA` as its schema.
#'
#' @param database `"postgres"`, `"mysql"` or `"duckdb"`.
#' @param profile The name of the profile in `io_config.yaml`. Write it as
#'   text in the block, so that Mage passes its settings.
#' @param ... Other arguments of [DBI::dbConnect()].
#' @return A DBI connection.
#' @export
#' @examples
#' \dontrun{
#' load_data <- function() {
#'   connection <- mageml::db_connect("postgres", profile = "default")
#'   on.exit(DBI::dbDisconnect(connection))
#'   DBI::dbGetQuery(connection, "SELECT * FROM users")
#' }
#' }
db_connect <- function(database = c("postgres", "mysql", "duckdb"),
                       profile = "default",
                       ...) {
  database <- match.arg(database)
  drivers <- c(postgres = "RPostgres", mysql = "RMariaDB", duckdb = "duckdb")
  require_packages(c("DBI", drivers[[database]]))
  arguments <- db_arguments(database, io_config(profile))
  driver <- switch(database,
    postgres = RPostgres::Postgres(),
    mysql = RMariaDB::MariaDB(),
    duckdb = duckdb::duckdb()
  )
  connection <- do.call(
    DBI::dbConnect,
    c(list(driver), arguments, list(...))
  )
  schema <- io_config(profile)$DUCKDB_SCHEMA
  if (database == "duckdb" && !is.null(schema) && nzchar(schema)) {
    quoted <- DBI::dbQuoteString(connection, schema)
    DBI::dbExecute(connection, paste("SET schema =", quoted))
  }
  connection
}
