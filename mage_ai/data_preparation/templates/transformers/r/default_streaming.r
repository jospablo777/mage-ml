#* @transformer
transform <- function(messages, ...) {
  # Runs on each batch of the stream's messages, as a tibble with one row per
  # JSON message, in an R session that stays up for the stream: code at the
  # top of the file, such as loading a model, runs once. Return the messages
  # to send on, one row each; NULL sends none. variable("name", default)
  # reads the pipeline's variables.
  minimum <- variable("minimum", 0)
  messages[messages$amount >= minimum, ]
}
