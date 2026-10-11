# Sets the package state a block run sets; restored when the calling test ends.
local_mageml_state <- function(block_type = NULL,
                               globals = list(),
                               .env = parent.frame()) {
  names <- c("block_type", "block_function", "globals")
  saved <- mget(names, envir = state, ifnotfound = list(NULL))
  withr::defer(
    for (name in names) assign(name, saved[[name]], envir = state),
    envir = .env
  )
  assign("block_type", block_type, envir = state)
  assign("block_function", NULL, envir = state)
  assign("globals", globals, envir = state)
  invisible(state)
}
