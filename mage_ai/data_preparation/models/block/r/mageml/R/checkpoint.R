#' Progress kept across a block run's attempts
#'
#' `checkpoint_save()` keeps a value for the next attempt of this block run,
#' and `checkpoint_load()` returns the value saved last, so a retried attempt,
#' or a retried pipeline run, resumes where the last one stopped instead of
#' starting over. When the block run completes, its checkpoints are removed.
#'
#' Values are JSON, at most 1 MB each: progress such as an offset, a cursor or
#' the files done, not data. A save replaces the file in one rename, so an
#' attempt stopped during a save leaves the value saved before. Python and Rust
#' blocks read and write the same files. Outside a pipeline run, in the
#' editor, checkpoints last for the one execution.
#'
#' @param key The checkpoint's name: 1 to 200 letters, digits, `-`, `_` or
#'   `.`, not starting with `.`.
#' @param value A value JSON can hold.
#' @param default The value when nothing was saved under `key`.
#' @return `checkpoint_load()` returns the saved value or `default`;
#'   `checkpoint_save()` returns `value`, invisibly.
#' @export
#' @examples
#' \dontrun{
#' start <- checkpoint_load("page", default = 0)
#' for (page in seq(start, pages - 1)) {
#'   write_page(fetch_page(page))
#'   checkpoint_save("page", page + 1)
#' }
#' }
checkpoint_load <- function(key, default = NULL) {
  check_checkpoint_key(key)
  dir <- checkpoint_dir()
  if (is.null(dir)) {
    text <- state$checkpoints[[key]]
  } else {
    path <- file.path(dir, paste0(key, ".json"))
    text <- if (file.exists(path)) {
      paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
    }
  }
  if (is.null(text)) {
    return(default)
  }
  jsonlite::fromJSON(text, simplifyVector = TRUE)
}

#' @rdname checkpoint_load
#' @export
checkpoint_save <- function(key, value) {
  check_checkpoint_key(key)
  text <- tryCatch(
    as.character(jsonlite::toJSON(
      value,
      auto_unbox = TRUE,
      digits = NA,
      null = "null",
      na = "null"
    )),
    error = function(e) {
      stop(
        sprintf(
          paste0(
            "Checkpoint %s takes a value JSON can hold, such as an offset or ",
            "the files done: %s"
          ),
          key,
          conditionMessage(e)
        ),
        call. = FALSE
      )
    }
  )
  if (nchar(text, type = "bytes") > 1024^2) {
    stop(
      sprintf(
        paste0(
          "Checkpoint %s is over 1 MB: keep progress in it (an offset, a ",
          "cursor), and the data in the block's output or a store."
        ),
        key
      ),
      call. = FALSE
    )
  }
  dir <- checkpoint_dir()
  if (is.null(dir)) {
    if (is.null(state$checkpoints)) {
      state$checkpoints <- list()
    }
    state$checkpoints[[key]] <- text
    return(invisible(value))
  }
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  temporary <- tempfile(pattern = paste0(".", key, "."), tmpdir = dir)
  writeLines(text, temporary, useBytes = TRUE)
  if (!file.rename(temporary, file.path(dir, paste0(key, ".json")))) {
    unlink(temporary)
    stop(sprintf("Saving checkpoint %s failed.", key), call. = FALSE)
  }
  invisible(value)
}

#' Stop on a key that is not a file name
#'
#' @param key A checkpoint's name.
#' @return `NULL`, invisibly.
#' @noRd
check_checkpoint_key <- function(key) {
  valid <- is.character(key) &&
    length(key) == 1 &&
    grepl("^[A-Za-z0-9_-][A-Za-z0-9._-]{0,199}$", key)
  if (!valid) {
    stop(
      sprintf(
        paste0(
          "A checkpoint key is 1 to 200 letters, digits, -, _ or . (not ",
          "first), not %s."
        ),
        deparse(key)
      ),
      call. = FALSE
    )
  }
  invisible(NULL)
}

#' The block run's checkpoints folder
#'
#' The run's `checkpoint_root` and the block's folder in it, named as MageML
#' and the services' worker name it (a `/` in the UUID becomes `__`).
#'
#' @return A path, or `NULL` outside a pipeline run.
#' @noRd
checkpoint_dir <- function() {
  root <- state$globals$checkpoint_root
  block <- state$context$block_uuid
  if (is.null(root) || is.null(block)) {
    return(NULL)
  }
  file.path(root, gsub("[/\\\\]", "__", block))
}
