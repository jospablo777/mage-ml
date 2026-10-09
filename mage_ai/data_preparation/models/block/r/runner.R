# Runs one R block. Mage calls it with Rscript --vanilla runner.R <job dir>.
# The library paths are set before any package loads. With an rv
# environment, they are its library in MAGE_R_LIBRARY, the library of the
# mageml package in MAGE_R_MAGEML_LIBRARY and R's base packages; site and
# user libraries are left out. Without one, the mageml library comes first.
args <- commandArgs(trailingOnly = TRUE)
rv_library <- Sys.getenv("MAGE_R_LIBRARY")
mageml_library <- Sys.getenv("MAGE_R_MAGEML_LIBRARY")
if (nzchar(rv_library)) {
  .libPaths(c(rv_library, mageml_library), include.site = FALSE)
} else {
  .libPaths(c(mageml_library, .libPaths()))
}
invisible(mageml::run_block(args[[1]]))
