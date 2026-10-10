register_block_function <- function(f, type) {
  if (!is.function(f)) {
    stop(sprintf("%s() takes a function.", type), call. = FALSE)
  }
  block_type <- state$block_type
  if (!is.null(block_type) && !identical(block_type, type)) {
    stop(
      sprintf(
        "This is a %s block; register its function with %s().",
        block_type,
        block_type
      ),
      call. = FALSE
    )
  }
  if (!is.null(state$block_function)) {
    stop(
      sprintf("The block registers more than one %s function.", type),
      call. = FALSE
    )
  }
  state$block_function <- f
  invisible(f)
}

#' Register the function of a block
#'
#' What the `#* @data_loader`, `#* @transformer` and `#* @data_exporter`
#' [annotations] do, from code. The function receives the outputs of the
#' upstream blocks, data frames as tibbles, in the order of the upstream
#' blocks. A data loader or transformer returns its output, usually a data
#' frame.
#'
#' A block registers one function.
#'
#' @param f The block's function.
#' @return `f`, invisibly.
#' @seealso [annotations], and [test()] for the block's tests.
#' @export
#' @examples
#' \dontrun{
#' library(mageml)
#'
#' transformer(function(df_1, ...) {
#'   dplyr::mutate(df_1, total = price * quantity)
#' })
#' }
data_loader <- function(f) {
  register_block_function(f, "data_loader")
}

#' @rdname data_loader
#' @export
transformer <- function(f) {
  register_block_function(f, "transformer")
}

#' @rdname data_loader
#' @export
data_exporter <- function(f) {
  register_block_function(f, "data_exporter")
}

#' @rdname data_loader
#' @export
custom <- function(f) {
  register_block_function(f, "custom")
}

#' Register a test of a block's output
#'
#' What the `#* @test` annotation does, from code. After the block's function
#' returns, Mage calls each test with its output, or with no arguments when
#' the test takes none. A test fails when it raises an error, as
#' [stopifnot()] and testthat's expectations do. The block fails when a test
#' fails; its output is stored first, as with Python blocks.
#'
#' @param f The test, a function of the block's output.
#' @param name The name the test is reported with. Defaults to the name of
#'   `f` when it is a variable, and to `test_<n>` otherwise.
#' @return `f`, invisibly.
#' @export
#' @examples
#' \dontrun{
#' test(function(output) {
#'   stopifnot(nrow(output) > 0, !anyNA(output$id))
#' })
#' }
test <- function(f, name = NULL) {
  if (is.null(name) && is.name(substitute(f))) {
    name <- as.character(substitute(f))
  }
  register_test(f, name)
}

register_test <- function(f, name = NULL) {
  if (!is.function(f)) {
    stop("test() takes a function.", call. = FALSE)
  }
  if (is.null(name)) {
    name <- paste0("test_", length(state$tests) + 1)
  }
  state$tests <- c(state$tests, list(list(name = name, f = f)))
  invisible(f)
}

#' Run the tests of a block
#'
#' @param value The block's output.
#' @return A list with the `name`, whether it `passed` and the error
#'   `message` of each test.
#' @noRd
run_tests <- function(value) {
  lapply(state$tests, function(test) {
    message <- tryCatch(
      {
        if (length(formals(test$f)) == 0) test$f() else test$f(value)
        NULL
      },
      error = function(e) conditionMessage(e)
    )
    list(name = test$name, passed = is.null(message), message = message)
  })
}

#' The pipeline's variables and the block's context
#'
#' `variable()` returns a variable of the pipeline run, such as
#' `execution_date` or a runtime variable. The block also has every variable
#' in `global_vars`. `context()` returns the block's UUID and type, its
#' pipeline's UUID and the execution partition.
#'
#' @param name The name of the variable.
#' @param default The value when the pipeline has no such variable. Without a
#'   default, a missing variable is an error.
#' @return The variable's value; for `context()`, a named list.
#' @export
#' @examples
#' \dontrun{
#' day <- as.Date(variable("execution_date"))
#' limit <- variable("limit", default = 100)
#' context()$block_uuid
#' }
variable <- function(name, default) {
  globals <- state$globals
  if (!is.null(globals) && name %in% names(globals)) {
    return(globals[[name]])
  }
  if (missing(default)) {
    stop(sprintf("The pipeline has no variable %s.", name), call. = FALSE)
  }
  default
}

#' @rdname variable
#' @export
context <- function() {
  state$context
}
