# Creates a job directory as Mage does, removed when the calling test ends.
local_job <- function(code,
                      block_type = "transformer",
                      inputs = list(),
                      globals = list(),
                      io_config = NULL,
                      .env = parent.frame()) {
  job_dir <- withr::local_tempdir(.local_envir = .env)
  entries <- lapply(seq_along(inputs), function(i) {
    value <- inputs[[i]]
    if (is.data.frame(value)) {
      path <- paste0("input_", i, ".arrow")
      arrow::write_ipc_file(value, file.path(job_dir, path))
      list(kind = "frame", path = path, json_columns = I(character(0)))
    } else {
      path <- paste0("input_", i, ".json")
      jsonlite::write_json(value, file.path(job_dir, path), auto_unbox = TRUE)
      list(kind = "json", path = path)
    }
  })
  manifest <- list(
    block_type = block_type,
    block_uuid = "a_block",
    pipeline_uuid = "a_pipeline",
    execution_partition = "1/20261009",
    inputs = entries
  )
  write_json <- function(value, name) {
    jsonlite::write_json(
      value,
      file.path(job_dir, name),
      auto_unbox = TRUE,
      null = "null"
    )
  }
  write_json(manifest, "manifest.json")
  # An empty named list, which jsonlite writes as {}.
  empty <- stats::setNames(list(), character(0))
  write_json(if (length(globals)) globals else empty, "globals.json")
  if (!is.null(io_config)) {
    write_json(io_config, "io_config.json")
  }
  writeLines(code, file.path(job_dir, "block.R"))
  job_dir
}

read_output <- function(job_dir) {
  out <- file.path(job_dir, "output")
  manifest <- jsonlite::read_json(file.path(out, "manifest.json"))
  value <- switch(manifest$kind,
    frame = arrow::read_ipc_file(
      file.path(out, "data.arrow"),
      as_data_frame = FALSE
    ),
    json = jsonlite::read_json(
      file.path(out, "data.json"),
      simplifyVector = TRUE
    ),
    none = NULL
  )
  list(
    kind = manifest$kind,
    json_columns = unlist(manifest$json_columns),
    value = value
  )
}

read_tests <- function(job_dir) {
  jsonlite::read_json(file.path(job_dir, "tests.json"), simplifyVector = FALSE)
}
