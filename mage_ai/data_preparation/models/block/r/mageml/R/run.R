function_names <- c(
  data_loader = "load_data",
  transformer = "transform",
  data_exporter = "export_data",
  custom = "custom"
)

#' Stop when packages are missing
#'
#' @param packages Names of packages.
#' @return `NULL`, invisibly, when every package is installed.
#' @noRd
require_packages <- function(packages) {
  installed <- vapply(packages, requireNamespace, logical(1), quietly = TRUE)
  missing <- packages[!installed]
  if (length(missing) > 0) {
    stop(
      sprintf(
        paste0(
          "R packages %s are not installed in the R environment. Add them ",
          "with `rv add %s` in the R project directory, or run `mage r init` ",
          "to create one."
        ),
        paste(missing, collapse = ", "),
        paste(missing, collapse = " ")
      ),
      call. = FALSE
    )
  }
  invisible(NULL)
}

#' Create the environment a block's code runs in
#'
#' Its parent holds mageml's exported functions, such as [transformer()] and
#' [test()], as Mage gives Python blocks its decorators; the parent of that is
#' the global environment, so packages the block attaches are found too.
#'
#' @return An environment.
#' @noRd
block_environment <- function() {
  exports <- new.env(parent = globalenv())
  namespace <- asNamespace("mageml")
  for (name in getNamespaceExports("mageml")) {
    assign(name, get(name, envir = namespace), envir = exports)
  }
  new.env(parent = exports)
}

#' Find the block's function
#'
#' @param env The environment the block's code ran in.
#' @param block_type The block's type.
#' @return The function the block registered, or the one it defined with the
#'   name of its type.
#' @noRd
block_function <- function(env, block_type) {
  if (!is.null(state$block_function)) {
    return(state$block_function)
  }
  function_name <- function_names[[block_type]]
  defined <- exists(
    function_name,
    envir = env,
    mode = "function",
    inherits = FALSE
  )
  if (!defined) {
    stop(
      sprintf(
        paste0(
          "The block has no function. Annotate it with #* @%s, register it ",
          "with %s(), or define %s()."
        ),
        block_type,
        block_type,
        function_name
      ),
      call. = FALSE
    )
  }
  get(function_name, envir = env)
}

#' Run a block's code and write its output and test results
#'
#' @param job_dir The job directory.
#' @return The value the block returned, invisibly.
#' @noRd
execute_block <- function(job_dir) {
  read_json <- function(name, ...) {
    jsonlite::read_json(file.path(job_dir, name), ...)
  }
  # int64 columns become integer64, which holds every value; by default
  # arrow turns them into doubles, which round integers above 2^53.
  old <- options(arrow.int64_downcast = FALSE)
  on.exit(options(old))

  manifest <- read_json("manifest.json", simplifyVector = FALSE)
  block_type <- manifest$block_type
  inputs <- lapply(manifest$inputs, read_input, job_dir = job_dir)
  globals <- read_json_file(file.path(job_dir, "globals.json"))
  config_path <- file.path(job_dir, "io_config.json")
  state$io_config <- if (file.exists(config_path)) {
    read_json("io_config.json", simplifyVector = TRUE)
  }
  state$globals <- globals
  state$block_type <- block_type
  state$block_function <- NULL
  state$tests <- list()
  state$context <- list(
    block_uuid = manifest$block_uuid,
    block_type = block_type,
    pipeline_uuid = manifest$pipeline_uuid,
    execution_partition = manifest$execution_partition
  )

  env <- block_environment()
  assign("global_vars", globals, envir = env)
  for (i in seq_along(inputs)) {
    assign(paste0("df_", i), inputs[[i]], envir = env)
  }
  path <- file.path(job_dir, "block.R")
  expressions <- parse(path, encoding = "UTF-8", keep.source = TRUE)
  evaluate_block(expressions, readLines(path, encoding = "UTF-8"), env)
  f <- block_function(env, block_type)
  value <- do.call(f, unname(inputs), envir = env)
  if (!identical(block_type, "data_exporter")) {
    write_output(value, job_dir)
  }
  jsonlite::write_json(
    run_tests(value),
    file.path(job_dir, "tests.json"),
    auto_unbox = TRUE,
    null = "null"
  )
  invisible(value)
}

write_lines <- function(lines, path) {
  connection <- file(path, open = "w", encoding = "UTF-8")
  on.exit(close(connection))
  writeLines(enc2utf8(lines), connection)
}

#' Run an R block
#'
#' Mage calls `run_block()` in a new `Rscript` process for each run of an R
#' block. The job directory holds:
#'
#' * `manifest.json`: the block type and one entry per input.
#' * `input_<n>.arrow` or `input_<n>.json`: the outputs of the upstream
#'   blocks, which the block receives as `df_1`, `df_2` and so on, and as the
#'   arguments of its function. Data frames arrive as tibbles.
#' * `globals.json`: the pipeline's variables, which the block reads from
#'   `global_vars`.
#' * `io_config.json`: the database settings that [db_connect()] uses.
#' * `block.R`: the block's code, whose function and tests are marked with
#'   [annotations], such as `#* @transformer` and `#* @test`.
#'
#' The value the block returns is written to `output/` and the results of
#' its tests to `tests.json`. When the block fails,
#' its error message is written to `error.txt` and its calls, with their
#' lines in `block.R`, to `traceback.txt`.
#'
#' @param job_dir The job directory.
#' @param quit_on_error Whether to quit R with status 1 when the block fails.
#' @return `TRUE` when the block ran, `FALSE` when it failed and
#'   `quit_on_error` is `FALSE`.
#' @keywords internal
#' @export
run_block <- function(job_dir, quit_on_error = TRUE) {
  calls <- NULL
  old <- options(keep.source = TRUE, warn = 1)
  on.exit(options(old))
  tryCatch(
    {
      withCallingHandlers(
        execute_block(job_dir),
        error = function(e) calls <<- sys.calls()
      )
      TRUE
    },
    error = function(e) {
      write_lines(conditionMessage(e), file.path(job_dir, "error.txt"))
      labels <- if (is.null(calls)) {
        character(0)
      } else {
        utils::limitedLabels(calls)
      }
      # The block's calls, with their lines; Mage's own calls are left out.
      labels <- labels[startsWith(labels, "block.R#")]
      write_lines(labels, file.path(job_dir, "traceback.txt"))
      if (quit_on_error) {
        quit(status = 1, save = "no")
      }
      FALSE
    }
  )
}
