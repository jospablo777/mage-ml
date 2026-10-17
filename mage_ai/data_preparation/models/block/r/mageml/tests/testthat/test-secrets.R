test_that("get_secret_value() returns passed secrets, names missing ones", {
  old <- state$secrets
  state$secrets <- list(api_token = "s3cr3t")
  withr::defer(state$secrets <- old)

  expect_equal(get_secret_value("api_token"), "s3cr3t")

  expect_error(get_secret_value("other"), "no secret other")
})
