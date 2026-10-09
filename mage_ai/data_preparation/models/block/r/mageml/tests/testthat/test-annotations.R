test_that("annotated functions are the block's function and tests", {
  job_dir <- local_job(
    c(
      "library(stats)",
      "",
      "#* @transformer",
      "double_x <- function(df_1, ...) {",
      "  df_1$x <- df_1$x * 2",
      "  df_1",
      "}",
      "",
      "#* @test",
      "has_rows <- function(output) stopifnot(nrow(output) == 2)",
      "",
      "#* A test with a description.",
      "#* @test",
      "x_is_even = function(output) stopifnot(all(output$x %% 2 == 0))",
      "",
      "#* @test",
      "function() stop(\"anonymous\")"
    ),
    inputs = list(data.frame(x = c(1, 3)))
  )

  expect_true(run_block(job_dir, quit_on_error = FALSE))

  expect_equal(read_output(job_dir)$value$x$as_vector(), c(2, 6))
  tests <- read_tests(job_dir)
  expect_equal(
    vapply(tests, `[[`, character(1), "name"),
    c("has_rows", "x_is_even", "test_3")
  )
  expect_equal(
    vapply(tests, `[[`, logical(1), "passed"),
    c(TRUE, TRUE, FALSE)
  )
  expect_equal(tests[[3]]$message, "anonymous")
})

test_that("an annotated anonymous function is the block's function", {
  job_dir <- local_job(
    c("#* @data_loader", "function() data.frame(id = 1:2)"),
    block_type = "data_loader"
  )

  expect_true(run_block(job_dir, quit_on_error = FALSE))

  expect_equal(read_output(job_dir)$value$id$as_vector(), 1:2)
})

test_that("unannotated functions are left alone", {
  job_dir <- local_job(c(
    "helper <- function(df) df",
    "",
    "#* @transformer",
    "main <- function(df_1) helper(df_1)",
    "",
    "# A plain comment.",
    "not_a_test <- function(output) stop(\"never runs\")"
  ), inputs = list(data.frame(x = 1)))

  expect_true(run_block(job_dir, quit_on_error = FALSE))

  expect_equal(read_tests(job_dir), list())
})

test_that("mistyped annotations are errors", {
  unknown <- local_job(c(
    "#* @transformer",
    "main <- function(df_1) df_1",
    "#* @tset",
    "check <- function(output) TRUE"
  ))
  roxygen <- local_job(c(
    "#' @transformer",
    "main <- function(df_1) df_1"
  ))
  not_function <- local_job(c(
    "#* @transformer",
    "main <- 5"
  ))
  wrong_type <- local_job(c(
    "#* @data_loader",
    "main <- function() 1"
  ))

  for (job_dir in c(unknown, roxygen, not_function, wrong_type)) {
    expect_false(run_block(job_dir, quit_on_error = FALSE))
  }

  error <- function(job_dir) readLines(file.path(job_dir, "error.txt"))
  expect_match(error(unknown), "line 3 has the unknown annotation @tset")
  expect_match(error(roxygen), "line 1 starts an annotation with #'")
  expect_match(error(not_function), "line 2, which is not a function")
  expect_match(error(wrong_type), "This is a transformer block")
})

test_that("roxygen comments of helper functions are fine", {
  job_dir <- local_job(c(
    "#' Double a column",
    "#' @param df A data frame.",
    "double <- function(df) df * 2",
    "",
    "#* @transformer",
    "main <- function(df_1) double(df_1)"
  ), inputs = list(data.frame(x = 1)))

  expect_true(run_block(job_dir, quit_on_error = FALSE))

  expect_equal(read_output(job_dir)$value$x$as_vector(), 2)
})

test_that("a block file runs in plain R without Mage", {
  path <- withr::local_tempfile(fileext = ".R")
  writeLines(c(
    "#* @transformer",
    "add_one <- function(df_1) { df_1$x <- df_1$x + 1; df_1 }"
  ), path)
  env <- new.env()

  sys.source(path, envir = env)

  expect_equal(env$add_one(data.frame(x = 1))$x, 2)
})
