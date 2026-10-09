frame <- function() {
  tibble::tibble(
    id = c(1L, 2L, NA),
    amount = c(1234.56789012, -0.000123456789, NA),
    name = c("ñ \"quoted\"", "it's", NA),
    day = as.Date(c("2024-01-31", "1900-01-01", NA)),
    flag = c(TRUE, FALSE, NA)
  )
}

test_that("each format round trips", {
  dir <- withr::local_tempdir()
  for (extension in c("csv", "tsv", "parquet", "feather", "rds", "json")) {
    path <- file.path(dir, "nested", paste0("data.", extension))
    expect_identical(write_file(frame(), path), path)
    back <- read_file(path)
    expect_s3_class(back, "tbl_df")
    expect_equal(nrow(back), 3)
    expect_equal(
      back$amount, frame()$amount,
      tolerance = 1e-12, info = extension
    )
    expect_equal(back$name, frame()$name, info = extension)
  }
})

test_that("JSON keeps every digit", {
  # jsonlite writes 4 significant digits unless told otherwise.
  path <- file.path(withr::local_tempdir(), "data.json")
  write_file(tibble::tibble(x = 1234.56789012), path)

  expect_equal(read_file(path)$x, 1234.56789012)
})

test_that("NDJSON round trips through arrow", {
  path <- file.path(withr::local_tempdir(), "data.ndjson")
  write_file(tibble::tibble(id = 1:2, name = c("a", "b")), path)

  back <- read_file(path)
  expect_equal(back$id, 1:2)
  expect_equal(back$name, c("a", "b"))
})

test_that("the format comes from the extension or the argument", {
  expect_equal(file_format("a/b.CSV"), "csv")
  expect_equal(file_format("b.csv.gz"), "csv")
  expect_equal(file_format("s3://bucket/b.pq"), "parquet")
  expect_equal(file_format("b.arrow"), "feather")
  expect_equal(file_format("b.data", format = "parquet"), "parquet")
  expect_error(file_format("b.data"), "Cannot tell the format")
})

test_that("S3 URIs and endpoints are parsed", {
  expect_equal(
    parse_s3_uri("s3://bucket/path/file.parquet"),
    list(bucket = "bucket", key = "path/file.parquet")
  )
  expect_error(parse_s3_uri("bucket/file"), "not an S3 URI")
  expect_equal(
    parse_endpoint("http://localhost:9000/"),
    list(scheme = "http", endpoint = "localhost:9000")
  )
  expect_equal(
    parse_endpoint("minio.example.com"),
    list(scheme = "https", endpoint = "minio.example.com")
  )
})

test_that("an S3 file system takes the settings of a profile", {
  withr::local_options(list())
  old <- state$io_config
  on.exit(state$io_config <- old)
  state$io_config <- list(minio = list(
    AWS_ACCESS_KEY_ID = "key",
    AWS_SECRET_ACCESS_KEY = "secret",
    AWS_REGION = "us-east-1",
    AWS_ENDPOINT = "http://127.0.0.1:9000"
  ))

  fs <- s3_filesystem("minio")
  expect_s3_class(fs, "S3FileSystem")
  expect_equal(fs$region, "us-east-1")
})
