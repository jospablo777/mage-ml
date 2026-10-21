raw_messages <- function(...) {
  lapply(c(...), charToRaw)
}

stream_step <- function(f, uuid = "block", send = TRUE, state_key = NULL) {
  list(
    uuid = uuid,
    f = f,
    env = globalenv(),
    context = list(),
    send = send,
    state_key = state_key,
    state = new.env(parent = emptyenv())
  )
}

test_that("messages become a tibble with every digit of large integers", {
  frame <- messages_frame(raw_messages(
    '{"amount": 1, "big": 1152921504606846977, "city": "Zürich"}',
    '{"amount": 3}'
  ))

  expect_s3_class(frame, "tbl_df")
  expect_type(frame$amount, "integer")
  expect_s3_class(frame$big, "integer64")
  expect_equal(format(frame$big[[1]]), "1152921504606846977")
  expect_equal(frame$city, c("Zürich", NA))
  expect_error(messages_frame(raw_messages("not json")), "not JSON objects")
})

test_that("rows, lists and NULL become messages", {
  rows <- frame_messages(data.frame(n = c(1L, 2L), x = c(0.1, NA)))
  expect_equal(
    vapply(rows, rawToChar, ""),
    c('{"n":1,"x":0.1}', '{"n":2,"x":null}')
  )

  listed <- frame_messages(list(list(a = 1), "text"))
  expect_equal(vapply(listed, rawToChar, ""), c('{"a":1}', '"text"'))

  expect_length(frame_messages(NULL), 0)
  expect_error(frame_messages(42), "returns a data frame")
})

test_that("blocks run in order and each sends its messages", {
  steps <- list(
    stream_step(function(m, ...) transform(m, n = n * 2), "double"),
    stream_step(function(m, ...) m[m$n > 2, ], "big")
  )

  result <- run_stream_batch(steps, raw_messages('{"n": 1}', '{"n": 3}'), FALSE)

  expect_true(result$reply$ok)
  expect_equal(
    vapply(result$outputs[[1]], rawToChar, ""),
    c('{"n":2}', '{"n":6}')
  )
  expect_equal(vapply(result$outputs[[2]], rawToChar, ""), '{"n":6}')
  expect_equal(result$reply$outputs[[2]]$count, 1)
})

test_that("failing messages are isolated; all failing fails the batch", {
  checks <- list(stream_step(function(m, ...) {
    if (is.null(m[["n"]]) || anyNA(m[["n"]])) stop("n is missing")
    m
  }, "check"))
  batch <- raw_messages('{"n": 1}', '{"x": 0}', '{"n": 3}')

  isolated <- run_stream_batch(checks, batch, TRUE)
  expect_true(isolated$reply$ok)
  expect_equal(
    vapply(isolated$outputs[[1]], rawToChar, ""),
    c('{"n":1}', '{"n":3}')
  )
  expect_equal(isolated$reply$failures[[1]]$row, 1L)
  expect_match(isolated$reply$failures[[1]]$message, "n is missing")

  failed <- run_stream_batch(checks, batch, FALSE)
  expect_false(failed$reply$ok)
  expect_equal(failed$reply$error$block_uuid, "check")

  all_failing <- run_stream_batch(
    checks,
    raw_messages('{"x": 0}', '{"y": 0}'),
    TRUE
  )
  expect_false(all_failing$reply$ok)
})

test_that("frames round trip and end cleanly", {
  out <- rawConnection(raw(0), "wb")
  write_frame(out, charToRaw("hello"))
  write_frame(out, raw(0))
  bytes <- rawConnectionValue(out)
  close(out)

  input <- rawConnection(bytes, "rb")
  on.exit(close(input))
  expect_equal(rawToChar(read_frame(input)), "hello")
  expect_equal(read_frame(input), raw(0))
  expect_null(read_frame(input))
})

count_cards <- function(m, ...) {
  if (is.null(m[["card"]]) || anyNA(m[["card"]])) stop("card is missing")
  m$seen <- vapply(m$card, function(card) {
    seen <- state_get(card, default = 0) + 1
    state_set(card, seen)
    seen
  }, numeric(1))
  if ("forget" %in% names(m)) {
    for (card in m$card[m$forget %in% TRUE]) state_remove(card)
  }
  m
}

test_that("a block keeps state per key and sends back what changed", {
  step <- stream_step(count_cards, "count", state_key = "card")
  restore_state(step, list(a = 4, b = 1, c = 9))

  result <- run_stream_batch(
    list(step),
    raw_messages(
      '{"card": "a"}',
      '{"card": "a"}',
      '{"card": "d"}',
      '{"card": "c", "forget": true}'
    ),
    FALSE
  )

  expect_true(result$reply$ok)
  expect_equal(
    vapply(
      result$outputs[[1]],
      function(m) jsonlite::fromJSON(rawToChar(m))$seen,
      numeric(1)
    ),
    c(5, 6, 1, 10)
  )
  updates <- result$reply$state_updates$count
  expect_equal(updates$a, 6)
  expect_equal(updates$d, 1)
  expect_true("c" %in% names(updates) && is.null(updates$c))
  expect_false("b" %in% names(updates))
  expect_equal(state_keys_of(step), c("a", "b", "d"))
  expect_match(
    as.character(jsonlite::toJSON(
      result$reply$state_updates,
      auto_unbox = TRUE,
      null = "null"
    )),
    '"c":null',
    fixed = TRUE
  )
})

test_that("a failing message leaves the state as it was", {
  step <- stream_step(count_cards, "count", state_key = "card")
  batch <- raw_messages('{"card": "a"}', '{"other": 1}', '{"card": "b"}')

  isolated <- run_stream_batch(list(step), batch, TRUE)
  expect_true(isolated$reply$ok)
  expect_equal(isolated$reply$failures[[1]]$row, 1L)
  expect_equal(isolated$reply$state_updates$count, list(a = 1, b = 1))

  restore_state(step, list())
  failed <- run_stream_batch(list(step), batch, FALSE)
  expect_false(failed$reply$ok)
  expect_null(failed$reply$state_updates)
  expect_length(state_keys_of(step), 0)
})

test_that("a block without a state_key cannot keep state", {
  step <- stream_step(count_cards, "count")
  result <- run_stream_batch(list(step), raw_messages('{"card": "a"}'), FALSE)
  expect_false(result$reply$ok)
  expect_match(result$reply$error$message, "has no state_key")
  expect_error(state_get("a"), "while they run")
})
