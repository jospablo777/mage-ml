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
#' 32-bit length and its bytes. A block with keyed state gets the values of
#' the batch's keys in the header and sends back what changed
#' ([state_get()]).
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
    request <- jsonlite::fromJSON(rawToChar(header), simplifyVector = FALSE)
    messages <- vector("list", request$count)
    for (i in seq_len(request$count)) {
      messages[[i]] <- read_frame(con)
    }
    for (step in steps) {
      restore_state(step, request$state[[step$uuid]])
      restore_timers(step, request$timers[[step$uuid]])
      step$batch$fired <- request$fired[[step$uuid]]
      step$batch$watermark <- request$watermark
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
#' @param step The block: `block_uuid`, `file`, `send` and `state_key`.
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
    send = isTRUE(step$send),
    state_key = step$state_key,
    state = new.env(parent = emptyenv()),
    timers = new.env(parent = emptyenv()),
    batch = new.env(parent = emptyenv())
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
  before <- snapshot_states(steps)
  # Timers that fired run first, on their own, so each fires once even when a
  # failing batch is split to find the messages that fail.
  fired <- NULL
  if (any(vapply(steps, function(s) length(s$batch$fired) > 0, TRUE))) {
    fired <- run_stream_chain(steps, list(), timers_pass = TRUE)
    for (step in steps) {
      step$batch$fired <- NULL
    }
    if (!is.null(fired$error)) {
      restore_states(steps, before)
      return(list(
        reply = list(ok = FALSE, error = fired$error),
        outputs = list()
      ))
    }
  }
  result <- run_stream_messages(steps, messages, isolate)
  if (!isTRUE(result$reply$ok)) {
    restore_states(steps, before)
    return(result)
  }
  if (!is.null(fired)) {
    result$outputs <- lapply(seq_along(result$outputs), function(i) {
      c(fired$outputs[[i]], result$outputs[[i]])
    })
    for (i in seq_along(result$reply$outputs)) {
      result$reply$outputs[[i]]$count <- length(result$outputs[[i]])
    }
    result$reply$blocks <- c(fired$blocks, result$reply$blocks)
  }
  updates <- state_updates(steps, before)
  if (length(updates$state) > 0) {
    result$reply$state_updates <- updates$state
  }
  if (length(updates$timers) > 0) {
    result$reply$timer_updates <- updates$timers
  }
  result
}

#' Run the blocks on a batch's messages
#'
#' @noRd
run_stream_messages <- function(steps, messages, isolate) {
  sent <- vapply(Filter(function(s) s$send, steps), function(s) s$uuid, "")
  before <- snapshot_states(steps)
  run <- run_stream_chain(steps, messages)
  if (!is.null(run$error)) {
    # The failed attempt's changes are undone; each message's run keeps its own.
    restore_states(steps, before)
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
run_stream_chain <- function(steps, messages, timers_pass = FALSE) {
  outputs <- list()
  timings <- list()
  current <- messages
  for (i in seq_along(steps)) {
    step <- steps[[i]]
    fired <- timers_pass && length(step$batch$fired) > 0
    # A block after one that kept nothing of the batch does not run; in the
    # timers pass, a block runs when its timers fired or the one before sent
    # messages.
    if (fired || length(current) > 0 || (i == 1 && !timers_pass)) {
      cat("\036mage-block ", step$uuid, "\n", sep = "")
      started <- Sys.time()
      state$context <- step$context
      state$keyed <- step
      value <- tryCatch(
        list(messages = call_stream_block(step, current)),
        error = function(e) {
          list(error = list(
            block_uuid = step$uuid,
            message = conditionMessage(e)
          ))
        }
      )
      state$keyed <- NULL
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
  before <- snapshot_states(steps)
  run <- run_stream_chain(steps, messages)
  sent <- sum(vapply(steps, function(s) s$send, TRUE))
  if (is.null(run$error)) {
    return(list(outputs = run$outputs, failures = list()))
  }
  # A failed run leaves the state as it was.
  restore_states(steps, before)
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

#' Keyed state of a stream block
#'
#' A stream block with `state_key` in its configuration keeps a value per key
#' between batches: a count per card, the last reading of a device. `state_key`
#' names the field of the source's messages that keys it. During a batch the
#' block holds the values of the keys of the batch's messages:
#' `state_get()` reads one, `state_set()` sets it, `state_remove()` removes
#' it and `state_keys()` lists them. Keys are text (`as.character()` for
#' numbers); values are anything JSON holds. A message that fails on its own
#' leaves the state as it was, and the engine saves the state with the
#' positions it reflects.
#'
#' @param key The key, such as a message's `card`.
#' @param default The value when the key has none.
#' @param value The key's new value.
#' @return `state_get()` the value or `default`; `state_keys()` a character
#'   vector; the others the state's previous value, invisibly.
#' @export
#' @examples
#' \dontrun{
#' transformer(function(messages) {
#'   messages$seen <- vapply(messages$card, function(card) {
#'     seen <- state_get(card, default = 0) + 1
#'     state_set(card, seen)
#'     seen
#'   }, numeric(1))
#'   messages
#' })
#' }
state_get <- function(key, default = NULL) {
  values <- keyed_values()
  key <- state_key_text(key)
  if (exists(key, envir = values, inherits = FALSE)) {
    get(key, envir = values, inherits = FALSE)
  } else {
    default
  }
}

#' @rdname state_get
#' @export
state_set <- function(key, value) {
  values <- keyed_values(change = TRUE)
  key <- state_key_text(key)
  previous <- state_get(key)
  assign(key, value, envir = values)
  invisible(previous)
}

#' @rdname state_get
#' @export
state_remove <- function(key) {
  values <- keyed_values()
  key <- state_key_text(key)
  previous <- state_get(key)
  if (exists(key, envir = values, inherits = FALSE)) {
    rm(list = key, envir = values)
  }
  invisible(previous)
}

#' @rdname state_get
#' @export
state_keys <- function() {
  sort(ls(keyed_values(), all.names = TRUE))
}

#' The running block's state values
#'
#' @noRd
keyed_values <- function(change = FALSE) {
  step <- state$keyed
  if (is.null(step)) {
    stop("Keyed state is for stream blocks while they run.", call. = FALSE)
  }
  if (change && is.null(step$state_key)) {
    stop(
      paste0(
        "Block ", step$uuid, " has no state_key in its configuration: name ",
        "the field of the messages that keys its state."
      ),
      call. = FALSE
    )
  }
  step$state
}

#' Timers of a stream block's keys
#'
#' A stream block with `state_key` sets a timer per key: a time in
#' milliseconds, event time when the stream has `time`, else the time batches
#' arrive. A timer fires once, when a batch of its partition moves the
#' watermark to it or past it: the block runs with no messages and the key in
#' `timers_fired()`, and the timer is removed unless the block sets it again.
#' `timer_get()` reads a key's timer, `timer_set()` sets it, `timer_remove()`
#' removes it, and `watermark()` is the time timers follow.
#'
#' @param key The key, such as a message's `device`.
#' @param at When the timer fires, in milliseconds since 1970.
#' @return `timer_get()` the time or `NULL`; `timers_fired()` a named numeric
#'   vector of the keys that fired and their times; `watermark()` a number or
#'   `NULL`; the others the previous time, invisibly.
#' @export
#' @examples
#' \dontrun{
#' transformer(function(messages) {
#'   fired <- timers_fired()
#'   alerts <- tibble::tibble(device = names(fired), silent_since = fired)
#'   for (i in seq_len(nrow(messages))) {
#'     state_set(messages$device[[i]], messages$at[[i]])
#'     timer_set(messages$device[[i]], messages$at[[i]] + 60000)
#'   }
#'   dplyr::bind_rows(alerts, messages)
#' })
#' }
timer_get <- function(key) {
  timers <- running_step()$timers
  key <- state_key_text(key)
  if (exists(key, envir = timers, inherits = FALSE)) {
    get(key, envir = timers, inherits = FALSE)
  } else {
    NULL
  }
}

#' @rdname timer_get
#' @export
timer_set <- function(key, at) {
  step <- running_step()
  keyed_values(change = TRUE)
  if (length(at) != 1 || !is.numeric(at) || !is.finite(at)) {
    stop("A timer is one time in milliseconds.", call. = FALSE)
  }
  key <- state_key_text(key)
  previous <- timer_get(key)
  assign(key, round(as.numeric(at)), envir = step$timers)
  invisible(previous)
}

#' @rdname timer_get
#' @export
timer_remove <- function(key) {
  step <- running_step()
  key <- state_key_text(key)
  previous <- timer_get(key)
  if (exists(key, envir = step$timers, inherits = FALSE)) {
    rm(list = key, envir = step$timers)
  }
  invisible(previous)
}

#' @rdname timer_get
#' @export
timers_fired <- function() {
  fired <- running_step()$batch$fired
  if (length(fired) == 0) {
    return(stats::setNames(numeric(0), character(0)))
  }
  unlist(fired)
}

#' @rdname timer_get
#' @export
watermark <- function() {
  running_step()$batch$watermark
}

#' @noRd
running_step <- function() {
  step <- state$keyed
  if (is.null(step)) {
    stop("Timers are for stream blocks while they run.", call. = FALSE)
  }
  step
}

#' @noRd
state_key_text <- function(key) {
  if (length(key) != 1 || is.na(key)) {
    stop("A state key is one value that is not NA.", call. = FALSE)
  }
  as.character(key)
}

#' Set a block's state values to those the engine sent
#'
#' @noRd
restore_state <- function(step, values) {
  rm(list = ls(step$state, all.names = TRUE), envir = step$state)
  if (length(values) > 0) {
    list2env(values, envir = step$state)
  }
  invisible(NULL)
}

#' Set a block's timers to those the engine sent
#'
#' @noRd
restore_timers <- function(step, times) {
  rm(list = ls(step$timers, all.names = TRUE), envir = step$timers)
  if (length(times) > 0) {
    list2env(times, envir = step$timers)
  }
  invisible(NULL)
}

#' @noRd
snapshot_states <- function(steps) {
  lapply(steps, function(step) {
    list(
      state = as.list(step$state, all.names = TRUE),
      timers = as.list(step$timers, all.names = TRUE)
    )
  })
}

#' @noRd
restore_states <- function(steps, snapshots) {
  for (i in seq_along(steps)) {
    restore_state(steps[[i]], snapshots[[i]]$state)
    restore_timers(steps[[i]], snapshots[[i]]$timers)
  }
}

#' Per block with keyed state, the keys and timers that changed: the new
#' value, or NULL (null) for a removed one
#'
#' @noRd
state_updates <- function(steps, snapshots) {
  updates <- list(state = list(), timers = list())
  for (i in seq_along(steps)) {
    step <- steps[[i]]
    if (is.null(step$state_key)) {
      next
    }
    for (kind in c("state", "timers")) {
      changed <- changed_keys(
        snapshots[[i]][[kind]],
        as.list(step[[kind]], all.names = TRUE)
      )
      if (length(changed) > 0) {
        updates[[kind]][[step$uuid]] <- changed
      }
    }
  }
  updates
}

#' @noRd
changed_keys <- function(before, after) {
  changed <- list()
  for (key in names(after)) {
    if (!key %in% names(before) || !identical(before[[key]], after[[key]])) {
      changed[key] <- list(after[[key]])
    }
  }
  for (key in setdiff(names(before), names(after))) {
    changed[key] <- list(NULL)
  }
  changed
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
