# Serves a stream's R blocks. The service engine (or MageML's editor) starts it with
# Rscript --vanilla stream_runner.R <directory>, the directory holding stream.json. The
# library paths are set as for runner.R before any package loads.
args <- commandArgs(trailingOnly = TRUE)
rv_library <- Sys.getenv("MAGE_R_LIBRARY")
mageml_library <- Sys.getenv("MAGE_R_MAGEML_LIBRARY")
if (nzchar(rv_library)) {
  .libPaths(c(rv_library, mageml_library), include.site = FALSE)
} else {
  .libPaths(c(mageml_library, .libPaths()))
}
Sys.setenv(R_LIBS = paste(.libPaths(), collapse = .Platform$path.sep))
invisible(mageml::serve_stream(args[[1]]))
