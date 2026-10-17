#' The value of a MageML secret
#'
#' Returns a secret saved in the MageML project, such as an API key. Mage
#' passes the secrets whose names appear in the block's code as
#' `get_secret_value("name")`, and no others.
#'
#' @param name The name of the secret, as text in the call.
#' @return The secret's value, as text.
#' @export
#' @examples
#' \dontrun{
#' token <- get_secret_value("api_token")
#' }
get_secret_value <- function(name) {
  value <- state$secrets[[name]]
  if (is.null(value)) {
    stop(
      sprintf(
        paste0(
          "There is no secret %s. Save it in MageML's secrets, and write ",
          "its name as text in the call, get_secret_value(\"%s\"), so Mage ",
          "passes it to the block."
        ),
        name,
        name
      ),
      call. = FALSE
    )
  }
  value
}
