#' Serve a stream's R blocks
#'
#' The resident R program of a streaming pipeline's R transformers, which the
#' service engine (and MageML's editor) starts with the stream: each block's
#' code runs once, then its function runs on each batch of messages, as a
#' tibble with one row per JSON message, and returns the messages to send on.
#'
#' The program connects to the port `stream.json` names and says it is ready,
#' with the token it holds. For each batch the engine sends a header frame and
#' one frame per message; the program answers with a header frame, then the
#' messages of each block the engine asked for. A frame is a little-endian
#' 32-bit length and its bytes.
#'
#' @param dir The directory that holds `stream.json`.
#' @return Does not return: R quits when the engine closes the connection.
#' @export
serve_stream <- function(dir) {
  job <- jsonlite::read_json(
    file.path(dir, "stream.json"),
    simplifyVector = FALSE
  )
  state$globals <- job$variables
  state$io_config <- job$io_config
  steps <- lapply(job$chain, load_stream_block, job = job)
  con <- socketConnection(
    host = "127.0.0.1",
    port = job$port,
    blocking = TRUE,
    open = "r+b",
    timeout = 86400
  )
  on.exit(close(con))
  write_frame(con, charToRaw(as.character(jsonlite::toJSON(
    list(type = "ready", protocol = 1L, token = job$token, pid = Sys.getpid()),
    auto_unbox = TRUE
  ))))
  flush(con)
  repeat {
    header <- read_frame(con)
    if (is.null(header)) {
      break
    }
    request <- jsonlite::fromJSON(rawToChar(header))
    messages <- vector("list", request$count)
    for (i in seq_len(request$count)) {
      messages[[i]] <- read_frame(con)
    }
    result <- run_stream_batch(steps, messages, isTRUE(job$isolate))
    reply <- c(list(id = request$id), result$reply)
    write_frame(con, charToRaw(as.character(
      jsonlite::toJSON(reply, auto_unbox = TRUE, null = "null", digits = NA)
    )))
    for (output in result$outputs) {
      for (message in output) {
        write_frame(con, message)
      }
    }
    flush(con)
    flush(stdout())
  }
  invisible(NULL)
}

#' Run a stream block's code once and keep its function
#'
#' @param step The block: `block_uuid`, `file` and `send`.
#' @param job The stream's job.
#' @return A list with the block's uuid, function and whether to send its
#'   messages.
#' @noRd
load_stream_block <- function(step, job) {
  state$block_type <- "transformer"
  state$block_function <- NULL
  state$tests <- list()
  state$context <- list(
    block_uuid = step$block_uuid,
    block_type = "transformer",
    pipeline_uuid = job$pipeline_uuid
  )
  env <- block_environment()
  assign("global_vars", job$variables, envir = env)
  expressions <- parse(step$file, encoding = "UTF-8", keep.source = TRUE)
  evaluate_block(expressions, readLines(step$file, encoding = "UTF-8"), env)
  list(
    uuid = step$block_uuid,
    f = block_function(env, "transformer"),
    env = env,
    context = state$context,
    send = isTRUE(step$send)
  )
}

#' Run the blocks on a batch, finding the messages that fail on their own
#'
#' @param steps The loaded blocks.
#' @param messages The batch's messages, raw vectors.
#' @param isolate Whether to find the messages that fail on their own.
#' @return A list with the reply's fields and the messages of each block to
#'   send.
#' @noRd
run_stream_batch <- function(steps, messages, isolate) {
  sent <- vapply(Filter(function(s) s$send, steps), function(s) s$uuid, "")
  run <- run_stream_chain(steps, messages)
  if (!is.null(run$error)) {
    failed <- list(
      reply = list(ok = FALSE, error = run$error),
      outputs = list()
    )
    if (!isolate || length(messages) <= 1) {
      return(failed)
    }
    isolated <- isolate_messages(steps, messages, 0L)
    if (length(isolated$failures) == length(messages)) {
      # Every message failing is a problem of the code, not of the messages.
      return(failed)
    }
    run <- list(
      outputs = isolated$outputs,
      blocks = list(),
      failures = isolated$failures
    )
  }
  counts <- lapply(seq_along(sent), function(i) {
    list(block_uuid = sent[[i]], count = length(run$outputs[[i]]))
  })
  list(
    reply = list(
      ok = TRUE,
      outputs = counts,
      blocks = run$blocks,
      failures = if (is.null(run$failures)) list() else run$failures
    ),
    outputs = run$outputs
  )
}

#' Run the blocks in order on the messages
#'
#' @return The messages of each block to send and each block's time, or the
#'   failing block's error.
#' @noRd
run_stream_chain <- function(steps, messages) {
  outputs <- list()
  timings <- list()
  current <- messages
  for (i in seq_along(steps)) {
    step <- steps[[i]]
    # A block after one that kept nothing of the batch does not run.
    if (i == 1 || length(current) > 0) {
      cat("\036mage-block ", step$uuid, "\n", sep = "")
      started <- Sys.time()
      state$context <- step$context
      value <- tryCatch(
        list(messages = call_stream_block(step, current)),
        error = function(e) {
          list(error = list(
            block_uuid = step$uuid,
            message = conditionMessage(e)
          ))
        }
      )
      if (!is.null(value$error)) {
        return(list(error = value$error))
      }
      timings[[length(timings) + 1]] <- list(
        block_uuid = step$uuid,
        seconds = as.numeric(Sys.time() - started, units = "secs")
      )
      current <- value$messages
    }
    if (step$send) {
      outputs[[length(outputs) + 1]] <- current
    }
  }
  list(outputs = outputs, blocks = timings)
}

#' Run a block's function on messages and return its messages as raw JSON
#'
#' @noRd
call_stream_block <- function(step, messages) {
  value <- do.call(step$f, list(messages_frame(messages)), envir = step$env)
  frame_messages(value)
}

#' A batch's messages as a tibble, one row per JSON object
#'
#' Arrow reads them: integers that fit stay R integers, larger ones become
#' integer64 with every digit (jsonlite would read them as doubles).
#'
#' @noRd
messages_frame <- function(messages) {
  if (length(messages) == 0) {
    return(tibble::tibble())
  }
  lines <- unlist(
    lapply(messages, function(m) c(m, as.raw(10L))),
    use.names = FALSE
  )
  rows <- tryCatch(
    arrow::read_json_arrow(lines, as_data_frame = TRUE),
    error = function(e) {
      stop(
        "The messages are not JSON objects; an R stream block gets one row ",
        "per object (",
        conditionMessage(e),
        ")",
        call. = FALSE
      )
    }
  )
  tibble::as_tibble(rows)
}

#' A block's result as messages: one JSON object per data frame row, each
#' element of a list, or nothing for NULL
#'
#' @noRd
frame_messages <- function(value) {
  if (is.null(value)) {
    return(list())
  }
  if (is.data.frame(value)) {
    if (nrow(value) == 0) {
      return(list())
    }
    lines <- character()
    out <- textConnection("lines", "w", local = TRUE)
    jsonlite::stream_out(value, out, digits = NA, na = "null", verbose = FALSE)
    close(out)
    return(lapply(lines, charToRaw))
  }
  if (is.list(value)) {
    return(lapply(value, function(item) {
      json <- jsonlite::toJSON(
        item,
        auto_unbox = TRUE,
        digits = NA,
        null = "null"
      )
      charToRaw(as.character(json))
    }))
  }
  stop(
    "An R stream block returns a data frame, a list of messages or NULL, not ",
    paste(class(value), collapse = "/"),
    call. = FALSE
  )
}

#' The outputs of the messages that run, and the rows of those that fail on
#' their own: a failing batch is split in halves until each failure is one
#' message
#'
#' @noRd
isolate_messages <- function(steps, messages, offset) {
  run <- run_stream_chain(steps, messages)
  sent <- sum(vapply(steps, function(s) s$send, TRUE))
  if (is.null(run$error)) {
    return(list(outputs = run$outputs, failures = list()))
  }
  if (length(messages) == 1) {
    failure <- c(list(row = offset), run$error)
    return(list(outputs = rep(list(list()), sent), failures = list(failure)))
  }
  middle <- length(messages) %/% 2
  left <- isolate_messages(steps, messages[seq_len(middle)], offset)
  right <- isolate_messages(
    steps,
    messages[seq.int(middle + 1, length(messages))],
    offset + middle
  )
  outputs <- lapply(
    seq_len(sent),
    function(i) c(left$outputs[[i]], right$outputs[[i]])
  )
  list(outputs = outputs, failures = c(left$failures, right$failures))
}

#' Write a frame: a little-endian 32-bit length, then the bytes
#'
#' @noRd
write_frame <- function(con, bytes) {
  writeBin(length(bytes), con, size = 4L, endian = "little")
  writeBin(bytes, con)
}

#' Read a frame, or NULL when the engine closed the connection between frames
#'
#' @noRd
read_frame <- function(con) {
  size <- readBin(con, "integer", n = 1L, size = 4L, endian = "little")
  if (length(size) == 0) {
    return(NULL)
  }
  if (size == 0) {
    return(raw(0))
  }
  chunks <- list()
  left <- size
  while (left > 0) {
    chunk <- readBin(con, "raw", n = left)
    if (length(chunk) == 0) {
      stop("The connection closed in the middle of a frame", call. = FALSE)
    }
    chunks[[length(chunks) + 1]] <- chunk
    left <- left - length(chunk)
  }
  do.call(c, chunks)
}
