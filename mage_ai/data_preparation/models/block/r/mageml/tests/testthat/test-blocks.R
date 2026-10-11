test_that("condition() registers the function of a conditional block", {
  local_mageml_state(block_type = "conditional")

  condition(function(...) TRUE)

  expect_true(is.function(state$block_function))
})

test_that("a function registers only for its block's type", {
  local_mageml_state(block_type = "transformer")

  expect_error(
    condition(function(...) TRUE),
    "register its function with transformer"
  )
})

test_that("variable() reads a pipeline variable or returns its default", {
  local_mageml_state(globals = list(minimum = 3))

  expect_equal(variable("minimum", 1), 3)
  expect_equal(variable("missing", "fallback"), "fallback")
})
