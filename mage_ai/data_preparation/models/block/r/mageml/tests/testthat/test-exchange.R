test_that("JSON text round-trips lists, with NA for NULL", {
  values <- list(list(a = 1, b = "x"), NULL, list(1, "a"))

  text <- to_json_text(values)

  expect_equal(text, c("{\"a\":1,\"b\":\"x\"}", NA, "[1,\"a\"]"))
  # jsonlite simplifies an array of numbers and text to a character vector.
  expect_equal(
    from_json_text(text),
    list(list(a = 1, b = "x"), NULL, c("1", "a"))
  )
})

test_that("inputs are tibbles, with JSON columns decoded", {
  job_dir <- withr::local_tempdir()
  df <- data.frame(id = 1:2, doc = c("{\"a\":[1,2]}", NA))
  arrow::write_ipc_file(df, file.path(job_dir, "input.arrow"))
  entry <- list(
    kind = "frame",
    path = "input.arrow",
    json_columns = list("doc")
  )

  result <- read_input(entry, job_dir)

  expect_s3_class(result, "tbl_df")
  expect_equal(result$doc, list(list(a = c(1, 2)), NULL))
})

test_that("64-bit integers keep every digit", {
  job_dir <- withr::local_tempdir()
  big <- arrow::Array$create(bit64::as.integer64("9007199254740993"))
  arrow::write_ipc_file(
    arrow::arrow_table(n = big),
    file.path(job_dir, "in.arrow")
  )
  withr::local_options(arrow.int64_downcast = FALSE)

  result <- read_input(list(kind = "frame", path = "in.arrow"), job_dir)
  write_output(result, job_dir)

  expect_s3_class(result$n, "integer64")
  expect_equal(as.character(result$n), "9007199254740993")
  column <- read_output(job_dir)$value$n
  expect_equal(column$type$ToString(), "int64")
  expect_equal(as.character(column$as_vector()), "9007199254740993")
})

test_that("naive date-times stay naive and zoned ones keep their zone", {
  table <- arrow_table(data.frame(
    naive = as.POSIXct("2024-01-01 12:00:00", tz = ""),
    zoned = as.POSIXct("2024-01-01 12:00:00", tz = "America/New_York"),
    local = as.POSIXlt("2024-01-01 12:00:00", tz = "UTC")
  ))$table

  expect_equal(table$schema$naive$type$ToString(), "timestamp[us]")
  expect_equal(
    table$schema$zoned$type$ToString(),
    "timestamp[us, tz=America/New_York]"
  )
  expect_equal(table$schema$local$type$ToString(), "timestamp[us, tz=UTC]")
})

test_that("durations and times of day keep their microseconds", {
  df <- data.frame(id = 1:2)
  df$span <- as.difftime(c(1.5, NA), units = "mins")
  df$time <- structure(
    c(45001.000001, NA),
    units = "secs",
    class = c("hms", "difftime")
  )

  table <- arrow_table(df)$table

  expect_equal(table$schema$span$type$ToString(), "duration[us]")
  expect_equal(table$schema$time$type$ToString(), "time64[us]")
  expect_equal(
    as.numeric(table$span$cast(arrow::int64())$as_vector()),
    c(90e6, NA)
  )
  expect_equal(
    as.numeric(table$time$cast(arrow::int64())$as_vector()),
    c(45001000001, NA)
  )
})

test_that("lists arrow cannot type become JSON text", {
  df <- data.frame(id = 1:2)
  df$tags <- list(c("a", "b"), character(0))
  df$mixed <- list(1, "a")
  df$records <- list(list(a = 1), NULL)
  df$frames <- list(data.frame(z = 1), NULL)

  converted <- arrow_table(df)

  expect_equal(converted$json_columns, c("mixed", "records"))
  expect_equal(
    converted$table$schema$tags$type$ToString(),
    "list<item: string>"
  )
  expect_equal(converted$table$mixed$as_vector(), c("1", "\"a\""))
  expect_equal(converted$table$records$as_vector(), c("{\"a\":1}", NA))
  expect_equal(
    converted$table$schema$frames$type$ToString(),
    "list<item: struct<z: double>>"
  )
})

test_that("outputs are frames, JSON values or nothing", {
  job_dir <- withr::local_tempdir()

  write_output(list(a = 1, b = c("x", NA)), job_dir)
  expect_equal(read_output(job_dir)$value, list(a = 1, b = c("x", NA)))

  write_output(NULL, job_dir)
  expect_equal(read_output(job_dir)$kind, "none")

  write_output(data.frame(id = integer(0)), job_dir)
  output <- read_output(job_dir)
  expect_equal(output$kind, "frame")
  expect_equal(output$value$num_rows, 0)
})

test_that("date-times keep every microsecond", {
  # 2040-04-09 18:49:26.747771 UTC, which a double holds as .74777099...
  micros <- bit64::as.integer64("2217610166747771")
  at <- as.POSIXct(as.numeric(micros) / 1e6, origin = "1970-01-01", tz = "UTC")

  table <- arrow_table(data.frame(at = at))$table

  stored <- table$at$cast(arrow::int64())$as_vector()
  expect_equal(as.character(stored), "2217610166747771")
})

test_that("integers Mage marks in JSON become integer64", {
  json <- paste0(
    "{\"one\": {\"$int64\": \"9007199254740993\"},",
    " \"many\": [{\"$int64\": \"-9007199254740993\"}, null],",
    " \"nested\": {\"a\": [1, {\"$int64\": \"9223372036854775807\"}]},",
    " \"small\": 5, \"text\": \"$int64\"}"
  )

  value <- parse_json(json)

  expect_s3_class(value$one, "integer64")
  expect_equal(as.character(value$one), "9007199254740993")
  expect_equal(as.character(value$many), c("-9007199254740993", NA))
  expect_equal(as.character(value$nested$a[[2]]), "9223372036854775807")
  expect_equal(value$nested$a[[1]], 1L)
  expect_equal(value$small, 5L)
  expect_equal(value$text, "$int64")
})

test_that("integer64 values are written to JSON exactly", {
  job_dir <- withr::local_tempdir()

  write_output(list(n = bit64::as.integer64("9007199254740993")), job_dir)

  text <- readLines(file.path(job_dir, "output", "data.json"))
  expect_equal(text, "{\"n\":9007199254740993}")
})

test_that("the last microsecond of 9999 stays in 9999", {
  # A double holds 9999-12-31 23:59:59.999999 as 10000-01-01.
  at <- as.POSIXct(253402300799.999999, origin = "1970-01-01", tz = "UTC")
  later <- as.POSIXct(253402300801, origin = "1970-01-01", tz = "UTC")

  micros <- datetime_microseconds(c(at, later, NA))

  expect_equal(
    as.character(micros),
    c("253402300799999999", "253402300801000000", NA)
  )
  expect_equal(
    sql_datetime(at, zone = TRUE),
    "9999-12-31 23:59:59.999999+00:00"
  )
})
