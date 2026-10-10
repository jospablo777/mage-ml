test_that("a registered transformer gets its inputs and writes its output", {
  job_dir <- local_job(
    c(
      "transformer(function(df_1, df_2, ...) {",
      "  df_1$y <- df_1$x * df_2$factor",
      "  df_1",
      "})"
    ),
    inputs = list(data.frame(x = c(1, 2)), list(factor = 10))
  )

  expect_true(run_block(job_dir, quit_on_error = FALSE))

  expect_equal(read_output(job_dir)$value$y$as_vector(), c(10, 20))
})

test_that("blocks may define their function by name", {
  job_dir <- local_job(
    "load_data <- function() data.frame(id = 1:3)",
    block_type = "data_loader"
  )

  expect_true(run_block(job_dir, quit_on_error = FALSE))

  expect_equal(read_output(job_dir)$value$id$as_vector(), 1:3)
})

test_that("blocks read the pipeline's variables and their context", {
  job_dir <- local_job(
    c(
      "data_loader(function() {",
      "  list(",
      "    day = variable(\"execution_date\"),",
      "    limit = variable(\"limit\", default = 5),",
      "    same = identical(",
      "      global_vars$execution_date,",
      "      variable(\"execution_date\")",
      "    ),",
      "    block = context()$block_uuid,",
      "    pipeline = context()$pipeline_uuid",
      "  )",
      "})"
    ),
    block_type = "data_loader",
    globals = list(execution_date = "2026-10-09T00:00:00")
  )

  expect_true(run_block(job_dir, quit_on_error = FALSE))

  expect_equal(
    read_output(job_dir)$value,
    list(
      day = "2026-10-09T00:00:00",
      limit = 5,
      same = TRUE,
      block = "a_block",
      pipeline = "a_pipeline"
    )
  )
})

test_that("a missing variable without a default is an error", {
  job_dir <- local_job(
    "data_loader(function() variable(\"missing\"))",
    block_type = "data_loader"
  )

  expect_false(run_block(job_dir, quit_on_error = FALSE))

  expect_equal(
    readLines(file.path(job_dir, "error.txt")),
    "The pipeline has no variable missing."
  )
})

test_that("errors are written with the block's calls and lines", {
  job_dir <- local_job(c(
    "check <- function(df) {",
    "  stop(\"no rows\")",
    "}",
    "transformer(function(df_1) {",
    "  check(df_1)",
    "})"
  ), inputs = list(data.frame(x = 1)))

  expect_false(run_block(job_dir, quit_on_error = FALSE))

  expect_equal(readLines(file.path(job_dir, "error.txt")), "no rows")
  calls <- readLines(file.path(job_dir, "traceback.txt"))
  expect_equal(
    calls,
    c("block.R#5: check(df_1)", "block.R#2: stop(\"no rows\")")
  )
  expect_false(file.exists(file.path(job_dir, "output")))
})

test_that("registering the wrong or a second function is an error", {
  wrong_type <- local_job("data_loader(function() 1)")
  second <- local_job(c(
    "transformer(function(df_1) df_1)",
    "transformer(function(df_1) df_1)"
  ))
  none <- local_job("x <- 1")

  for (job_dir in c(wrong_type, second, none)) {
    expect_false(run_block(job_dir, quit_on_error = FALSE))
  }

  error <- function(job_dir) readLines(file.path(job_dir, "error.txt"))
  expect_match(error(wrong_type), "This is a transformer block")
  expect_match(error(second), "more than one transformer function")
  expect_match(error(none), "Annotate it with #\\* @transformer")
})

test_that("exporters write no output", {
  job_dir <- local_job(
    "data_exporter(function(df_1) nrow(df_1))",
    block_type = "data_exporter",
    inputs = list(data.frame(x = 1:2))
  )

  expect_true(run_block(job_dir, quit_on_error = FALSE))

  expect_false(file.exists(file.path(job_dir, "output")))
  expect_true(file.exists(file.path(job_dir, "tests.json")))
})

test_that("tests run with the output and report failures", {
  job_dir <- local_job(c(
    "transformer(function(df_1) df_1)",
    "has_rows <- function(output) stopifnot(nrow(output) == 2)",
    "test(has_rows)",
    "test(function(output) stopifnot(\"no NA in x\" = !anyNA(output$x)))",
    "test(function() TRUE, name = \"takes_nothing\")"
  ), inputs = list(data.frame(x = c(1, NA))))

  expect_true(run_block(job_dir, quit_on_error = FALSE))

  expect_equal(read_tests(job_dir), list(
    list(name = "has_rows", passed = TRUE, message = NULL),
    list(name = "test_2", passed = FALSE, message = "no NA in x"),
    list(name = "takes_nothing", passed = TRUE, message = NULL)
  ))
})

test_that("missing packages are named with the rv command that adds them", {
  expect_error(
    require_packages(c("jsonlite", "notapackage1", "notapackage2")),
    paste0(
      "notapackage1, notapackage2 are not installed.*",
      "rv add notapackage1 notapackage2"
    )
  )
})

test_that("custom blocks run by annotation, registration or name", {
  annotated <- local_job(
    c(
      "#* @custom",
      "summarise_inputs <- function(df_1, ...) {",
      "  list(rows = nrow(df_1))",
      "}"
    ),
    block_type = "custom",
    inputs = list(data.frame(x = 1:4))
  )
  expect_true(run_block(annotated, quit_on_error = FALSE))
  expect_equal(read_output(annotated)$value$rows, 4)

  registered <- local_job(
    "custom(function(...) data.frame(id = 1:2))",
    block_type = "custom"
  )
  expect_true(run_block(registered, quit_on_error = FALSE))
  expect_equal(read_output(registered)$value$id$as_vector(), 1:2)

  named <- local_job("custom <- function(...) 'done'", block_type = "custom")
  expect_true(run_block(named, quit_on_error = FALSE))
  expect_equal(read_output(named)$value, "done")
})
