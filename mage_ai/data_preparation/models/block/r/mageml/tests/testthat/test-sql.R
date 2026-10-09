test_that("dates keep four-digit years", {
  dates <- as.Date(c("0001-01-01", "0999-12-31", "2024-02-29", NA))

  expect_equal(
    sql_date(dates),
    c("0001-01-01", "0999-12-31", "2024-02-29", NA)
  )
})

test_that("date-times keep every microsecond", {
  micros <- c("1704112496789012", "-181314200540109", "0")
  at <- as.POSIXct(
    as.numeric(bit64::as.integer64(micros)) / 1e6,
    origin = "1970-01-01",
    tz = "UTC"
  )
  at <- c(at, as.POSIXct(NA, tz = "UTC"))

  expect_equal(sql_datetime(at, zone = TRUE), c(
    "2024-01-01 12:34:56.789012+00:00",
    "1964-04-03 10:56:39.459891+00:00",
    "1970-01-01 00:00:00.000000+00:00",
    NA
  ))
  expect_equal(
    sql_datetime(at[3], zone = FALSE),
    "1970-01-01 00:00:00.000000"
  )
})

test_that("values are prepared with the column types that read them", {
  df <- data.frame(id = 1:2)
  df$day <- as.Date(c("0001-01-01", NA))
  df$naive <- as.POSIXct(c("2024-01-01 12:00:00", NA), tz = "")
  df$zoned <- as.POSIXct(c("2024-01-01 12:00:00", NA), tz = "America/New_York")
  df$tags <- list(c("a", "b"), NULL)
  df$raw <- structure(
    list(as.raw(c(0, 255)), NULL),
    class = c("arrow_binary", "vctrs_vctr", "list")
  )

  prepared <- sql_values(df, "postgres")

  expect_equal(prepared$field_types, c(
    day = "date",
    naive = "timestamp",
    zoned = "timestamptz"
  ))
  expect_equal(prepared$df$day, c("0001-01-01", NA))
  expect_equal(prepared$df$naive, c("2024-01-01 12:00:00.000000", NA))
  expect_equal(prepared$df$zoned, c("2024-01-01 17:00:00.000000+00:00", NA))
  expect_equal(prepared$df$tags, c("[\"a\",\"b\"]", NA))
  expect_s3_class(prepared$df$raw, "blob")
  expect_equal(unclass(prepared$df$raw)[[1]], as.raw(c(0, 255)))
  expect_null(unclass(prepared$df$raw)[[2]])

  mysql <- sql_values(df, "mysql")
  expect_equal(mysql$df$zoned, c("2024-01-01 17:00:00.000000", NA))
  expect_equal(mysql$field_types[["zoned"]], "DATETIME(6)")
})
