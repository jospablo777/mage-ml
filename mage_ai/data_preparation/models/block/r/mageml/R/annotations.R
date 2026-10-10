#' Annotate a block's functions
#'
#' R has no decorators. Like plumber, Mage reads comments that start with
#' `#*` above a function, the counterparts of Mage's Python decorators:
#'
#' * `#* @data_loader`, `#* @transformer`, `#* @data_exporter` or `#* @custom`
#'   marks the block's function. A block has one, of its own type.
#' * `#* @test` marks a test of the block's output. Mage calls it with the
#'   output, or with no arguments when it takes none; it fails when it raises
#'   an error. It is reported with the function's name.
#'
#' Annotations are comments, so the block's file runs in any R session, where
#' its functions can be called and tested on their own.
#'
#' [data_loader()], [transformer()], [data_exporter()] and [test()] register
#' functions the same way from code. Blocks may also define a function named
#' `load_data()`, `transform()` or `export_data()`, as earlier versions of
#' Mage required.
#'
#' @name annotations
#' @examples
#' \dontrun{
#' library(tidyverse)
#'
#' #* @transformer
#' add_totals <- function(orders, ...) {
#'   orders |> mutate(total = price * quantity)
#' }
#'
#' #* @test
#' totals_are_positive <- function(output) {
#'   stopifnot(all(output$total > 0, na.rm = TRUE))
#' }
#' }
NULL

annotation_tags <- c(
  "data_loader", "transformer", "data_exporter", "custom", "test"
)

#' Read the annotations above each of a block's expressions
#'
#' @param expressions The block's expressions, parsed with their source.
#' @param lines The lines of the block's file.
#' @return A list with the tags above each expression.
#' @noRd
read_annotations <- function(expressions, lines) {
  check_roxygen_annotations(lines)
  srcrefs <- attr(expressions, "srcref")
  lapply(seq_along(expressions), function(i) {
    tags <- character(0)
    line <- srcrefs[[i]][[1]] - 1
    while (line >= 1 && grepl("^\\s*#\\*", lines[[line]])) {
      found <- regmatches(
        lines[[line]],
        gregexpr("@[A-Za-z_][A-Za-z0-9_.]*", lines[[line]])
      )[[1]]
      found <- sub("^@", "", found)
      unknown <- setdiff(found, annotation_tags)
      if (length(unknown) > 0) {
        stop(
          sprintf(
            paste0(
              "block.R line %d has the unknown annotation @%s. Mage reads ",
              "@data_loader, @transformer, @data_exporter, @custom and @test."
            ),
            line,
            unknown[[1]]
          ),
          call. = FALSE
        )
      }
      tags <- c(found, tags)
      line <- line - 1
    }
    tags
  })
}

# A roxygen comment with a tag of Mage's is most likely a mistyped
# annotation, which would leave a function or test out silently.
check_roxygen_annotations <- function(lines) {
  pattern <- paste0(
    "^\\s*#'\\s*@(",
    paste(annotation_tags, collapse = "|"),
    ")\\b"
  )
  found <- grep(pattern, lines)
  if (length(found) > 0) {
    stop(
      sprintf(
        paste0(
          "block.R line %d starts an annotation with #'. Mage reads ",
          "annotations that start with #*, such as #* @transformer."
        ),
        found[[1]]
      ),
      call. = FALSE
    )
  }
}

#' The name an expression assigns to
#'
#' @param expression An expression of the block.
#' @return The name for `name <- value` or `name = value`, otherwise `NULL`.
#' @noRd
assigned_name <- function(expression) {
  assignment <- is.call(expression) &&
    is.name(expression[[1]]) &&
    as.character(expression[[1]]) %in% c("<-", "=") &&
    is.name(expression[[2]])
  if (assignment) as.character(expression[[2]]) else NULL
}

#' Evaluate a block's code and register its annotated functions
#'
#' @param expressions The block's expressions, parsed with their source.
#' @param lines The lines of the block's file.
#' @param env The environment the block runs in.
#' @noRd
evaluate_block <- function(expressions, lines, env) {
  annotations <- read_annotations(expressions, lines)
  srcrefs <- attr(expressions, "srcref")
  for (i in seq_along(expressions)) {
    expression <- expressions[[i]]
    value <- eval(expression, envir = env)
    tags <- annotations[[i]]
    if (length(tags) == 0) {
      next
    }
    name <- assigned_name(expression)
    f <- if (is.null(name)) value else get(name, envir = env)
    if (!is.function(f)) {
      stop(
        sprintf(
          "@%s annotates block.R line %d, which is not a function.",
          tags[[1]],
          srcrefs[[i]][[1]]
        ),
        call. = FALSE
      )
    }
    for (tag in tags) {
      if (tag == "test") {
        register_test(f, name)
      } else {
        register_block_function(f, tag)
      }
    }
  }
  invisible(NULL)
}
