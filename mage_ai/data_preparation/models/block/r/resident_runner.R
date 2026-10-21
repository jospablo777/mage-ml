# Serves the R blocks of many runs in one R session, for a pipeline service: R and the
# packages loaded once stay loaded. Mage starts it with Rscript --vanilla
# resident_runner.R; each line on stdin is a job directory, as runner.R takes one, and
# after each job the session prints the done marker. A block's error is written to the
# job directory as runner.R writes it, and the session goes on.
rv_library <- Sys.getenv("MAGE_R_LIBRARY")
mageml_library <- Sys.getenv("MAGE_R_MAGEML_LIBRARY")
if (nzchar(rv_library)) {
  .libPaths(c(rv_library, mageml_library), include.site = FALSE)
} else {
  .libPaths(c(mageml_library, .libPaths()))
}
Sys.setenv(R_LIBS = paste(.libPaths(), collapse = .Platform$path.sep))
input <- file("stdin", open = "r")
repeat {
  job_dir <- readLines(input, n = 1)
  if (length(job_dir) == 0) {
    break
  }
  manifest <- jsonlite::read_json(file.path(job_dir, "manifest.json"))
  if (!is.null(manifest$chain)) {
    invisible(mageml::run_chain(job_dir, quit_on_error = FALSE))
  } else {
    invisible(mageml::run_block(job_dir, quit_on_error = FALSE))
  }
  cat("\u001emage-job-done\n")
  flush(stdout())
}
