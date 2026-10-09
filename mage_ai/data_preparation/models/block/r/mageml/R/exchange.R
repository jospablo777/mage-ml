# Data frames cross between Mage and R blocks as Arrow IPC files, and other
# values as JSON. Mage casts columns before R reads them where arrow would
# not read them exactly; see exchange.py in Mage.

#' Decode a column of JSON text
#'
#' Mage sends struct, map and mixed columns as JSON text, since arrow turns a
#' `NULL` struct into a row of fields, with `""` in text fields. JSON arrays
#' of one type become vectors, and objects named lists; jsonlite turns arrays
#' of numbers and text into character vectors.
#'
#' @param values A character vector of JSON text, with `NA` for `NULL`.
#' @return A list with one decoded value per element, and `NULL` for `NA`.
#' @noRd
from_json_text <- function(values) {
  lapply(values, function(value) {
    if (is.na(value)) NULL else parse_json(value)
  })
}

#' Decode the integers Mage marks in JSON
#'
#' jsonlite reads JSON numbers as doubles, so Mage writes integers that a
#' double cannot hold as `{"$int64": "<digits>"}`. They become
#' `bit64::integer64`.
#'
#' @param x A value decoded by jsonlite with `simplifyVector = TRUE`.
#' @return `x` with the marked integers as `integer64`.
#' @noRd
decode_int64 <- function(x) {
  marked <- identical(names(x), "$int64")
  if (is.data.frame(x)) {
    if (marked) {
      return(bit64::as.integer64(x[["$int64"]]))
    }
    x[] <- lapply(x, decode_int64)
  } else if (is.list(x)) {
    if (marked) {
      return(bit64::as.integer64(x[["$int64"]]))
    }
    for (i in seq_along(x)) {
      if (!is.null(x[[i]])) {
        x[[i]] <- decode_int64(x[[i]])
      }
    }
  }
  x
}

#' Read JSON that Mage wrote
#'
#' @param json JSON text, or a path with `read_json()`.
#' @noRd
parse_json <- function(json) {
  decode_int64(jsonlite::fromJSON(json, simplifyVector = TRUE))
}

read_json_file <- function(path) {
  decode_int64(jsonlite::read_json(path, simplifyVector = TRUE))
}

#' Encode a list column as JSON text
#'
#' @param values A list.
#' @return A character vector with one JSON text per element, and `NA` for
#'   `NULL`.
#' @noRd
to_json_text <- function(values) {
  to_json <- function(value) {
    if (is.null(value)) {
      return(NA_character_)
    }
    value |>
      jsonlite::toJSON(
        auto_unbox = TRUE,
        null = "null",
        na = "null",
        digits = NA
      ) |>
      as.character()
  }
  vapply(values, to_json, character(1), USE.NAMES = FALSE)
}

#' Read an input of a block
#'
#' @param entry An input of the job's manifest: its `path`, its `kind`,
#'   `"frame"` or `"json"`, and the `json_columns` of a frame.
#' @param job_dir The job directory.
#' @return A tibble for a frame; otherwise the value decoded from JSON.
#' @noRd
read_input <- function(entry, job_dir) {
  path <- file.path(job_dir, entry$path)
  if (!identical(entry$kind, "frame")) {
    return(read_json_file(path))
  }
  df <- as.data.frame(arrow::read_ipc_file(path), stringsAsFactors = FALSE)
  for (column in unlist(entry$json_columns)) {
    df[[column]] <- from_json_text(df[[column]])
  }
  tibble::as_tibble(df)
}

#' Seconds as an Arrow array of microseconds
#'
#' arrow truncates the seconds of date-times, durations and times of day,
#' which doubles hold to within half a microsecond, so 0.747771 seconds held
#' as 0.74777099... became 747770 microseconds. Rounding keeps every
#' microsecond of date-times up to the year 2242.
#'
#' @param seconds A numeric vector of seconds.
#' @param type The Arrow type to cast the microseconds to.
#' @noRd
microseconds <- function(seconds, type) {
  arrow::Array$create(bit64::as.integer64(round(seconds * 1e6)))$cast(type)
}

# 9999-12-31 23:59:59.999999 UTC, the last microsecond Python and databases
# hold, in microseconds.
last_microsecond <- bit64::as.integer64("253402300799999999")

#' Date-times as microseconds since 1970
#'
#' Doubles hold date-times near the year 10000 to about 30 microseconds, so
#' 9999-12-31 23:59:59.999999, a common value for "no end", rounded to
#' 10000-01-01, which Python and databases cannot hold. Values less than a
#' millisecond past the last microsecond are that microsecond.
#'
#' @param x A `POSIXct` vector.
#' @return An `integer64` vector.
#' @noRd
datetime_microseconds <- function(x) {
  micros <- bit64::as.integer64(round(unclass(as.numeric(x)) * 1e6))
  past_end <- !is.na(micros) &
    micros > last_microsecond &
    micros <= last_microsecond + 1000L
  micros[past_end] <- last_microsecond
  micros
}

is_naive <- function(x) {
  !nzchar(paste(attr(x, "tzone"), collapse = ""))
}

#' Whether a list column crosses as JSON text
#'
#' arrow cannot type lists of mixed values, and it types lists of named lists
#' as lists of their values, dropping the names. Lists of vectors and of data
#' frames stay Arrow lists.
#'
#' @param x A list.
#' @noRd
needs_json <- function(x) {
  nested <- vapply(
    x,
    function(value) is.list(value) && !is.data.frame(value),
    logical(1)
  )
  if (any(nested)) {
    return(TRUE)
  }
  tryCatch(
    {
      arrow::Array$create(x)
      FALSE
    },
    error = function(e) TRUE
  )
}

#' Convert a data frame to an Arrow table for Mage
#'
#' Naive date-times stay naive, date-times, durations and times of day keep
#' their microseconds, and list columns of mixed values or of lists become
#' JSON text.
#'
#' @param value A data frame.
#' @return A list with the Arrow `table` and the names of its `json_columns`.
#' @noRd
arrow_table <- function(value) {
  value <- as.data.frame(value, stringsAsFactors = FALSE, optional = TRUE)
  json_columns <- character(0)
  for (column in names(value)) {
    x <- value[[column]]
    if (inherits(x, "POSIXlt")) {
      x <- as.POSIXct(x)
    }
    if (inherits(x, "POSIXct") && is_naive(x)) {
      # arrow writes a POSIXct without a tzone attribute in the session's
      # time zone, UTC, and one with an empty tzone as naive, which is how
      # naive timestamps came in.
      attr(x, "tzone") <- ""
    }
    if (is.list(x) && !is.data.frame(x) && needs_json(x)) {
      x <- to_json_text(x)
      json_columns <- c(json_columns, column)
    }
    value[[column]] <- x
  }
  table <- arrow::as_arrow_table(value)
  for (column in names(value)) {
    x <- value[[column]]
    # arrow writes durations and times of day as whole seconds.
    if (inherits(x, "hms")) {
      seconds <- as.numeric(x, units = "secs")
      table[[column]] <- microseconds(seconds, arrow::time64("us"))
    } else if (inherits(x, "difftime")) {
      seconds <- as.numeric(x, units = "secs")
      table[[column]] <- microseconds(seconds, arrow::duration("us"))
    } else if (inherits(x, "POSIXct")) {
      zone <- attr(x, "tzone")
      type <- if (is_naive(x)) {
        arrow::timestamp("us")
      } else {
        arrow::timestamp("us", zone[[1]])
      }
      micros <- arrow::Array$create(datetime_microseconds(x))
      table[[column]] <- micros$cast(type)
    }
  }
  list(table = table, json_columns = json_columns)
}

#' Write the value a block returned
#'
#' Writes `output/data.arrow` for a data frame, `output/data.json` for other
#' values, and `output/manifest.json`, which says which one it wrote.
#'
#' @param value The value the block returned.
#' @param job_dir The job directory.
#' @noRd
write_output <- function(value, job_dir) {
  out <- file.path(job_dir, "output")
  dir.create(out, showWarnings = FALSE)
  json_columns <- character(0)
  if (is.null(value)) {
    kind <- "none"
  } else if (is.data.frame(value)) {
    converted <- arrow_table(value)
    arrow::write_ipc_file(converted$table, file.path(out, "data.arrow"))
    json_columns <- converted$json_columns
    kind <- "frame"
  } else {
    jsonlite::write_json(
      value,
      file.path(out, "data.json"),
      auto_unbox = TRUE,
      digits = NA,
      null = "null",
      na = "null"
    )
    kind <- "json"
  }
  jsonlite::write_json(
    list(kind = kind, json_columns = I(json_columns)),
    file.path(out, "manifest.json"),
    auto_unbox = TRUE
  )
}
