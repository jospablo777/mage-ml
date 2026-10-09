#' mageml: run R blocks in Mage ML pipelines
#'
#' Mage runs each R block in its own `Rscript` process, which calls
#' [run_block()] with a job directory. The job directory holds the block's
#' code, the outputs of its upstream blocks and the pipeline's variables.
#'
#' A block marks its function and tests with [annotations], such as
#' `#* @transformer` and `#* @test`. The outputs of the upstream blocks are
#' the arguments of the block's function, data frames as tibbles. The
#' pipeline's variables are in `global_vars` and [variable()].
#'
#' [read_sql()], [write_table()] and [db_connect()] use the databases of the
#' project's `io_config.yaml`.
#'
#' @keywords internal
"_PACKAGE"

# What run_block() reads for the other functions, such as the database
# settings that db_connect() uses.
state <- new.env(parent = emptyenv())
