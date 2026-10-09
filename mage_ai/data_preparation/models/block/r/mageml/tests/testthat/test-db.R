test_that("io_config() names the profile it lacks", {
  old <- state$io_config
  state$io_config <- list(default = list(POSTGRES_HOST = "localhost"))
  withr::defer(state$io_config <- old)

  expect_equal(io_config(), list(POSTGRES_HOST = "localhost"))

  expect_error(io_config("warehouse"), "no profile warehouse")
})

test_that("Postgres settings become RPostgres arguments", {
  config <- list(
    POSTGRES_DBNAME = "db",
    POSTGRES_HOST = "localhost",
    POSTGRES_PORT = 5432L,
    POSTGRES_USER = "user",
    POSTGRES_PASSWORD = "p:a/s@s",
    POSTGRES_SCHEMA = "analytics",
    POSTGRES_CONNECT_TIMEOUT = "",
    MYSQL_HOST = "elsewhere"
  )

  expect_equal(db_arguments("postgres", config), list(
    dbname = "db",
    host = "localhost",
    port = 5432L,
    user = "user",
    password = "p:a/s@s",
    options = "-c search_path=analytics"
  ))
})

test_that("MySQL and DuckDB settings become driver arguments", {
  mysql <- list(MYSQL_DATABASE = "db", MYSQL_HOST = "h", MYSQL_PORT = "3306")
  duckdb <- list(DUCKDB_DATABASE = "file.duckdb", DUCKDB_SCHEMA = "main")

  expect_equal(
    db_arguments("mysql", mysql),
    list(dbname = "db", host = "h", port = 3306L)
  )
  expect_equal(db_arguments("duckdb", duckdb), list(dbdir = "file.duckdb"))
})

test_that("SSH tunnels are refused", {
  config <- list(POSTGRES_CONNECTION_METHOD = "ssh_tunnel")

  expect_error(
    db_arguments("postgres", config),
    "direct Postgres connections only"
  )
})
