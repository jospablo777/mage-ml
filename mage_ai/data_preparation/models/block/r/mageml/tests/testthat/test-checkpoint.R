test_that("a saved value is there for the next attempt of the block run", {
  root <- withr::local_tempdir()
  local_mageml_state(
    globals = list(checkpoint_root = root),
    context = list(block_uuid = "folder/pages")
  )

  expect_equal(checkpoint_load("page", default = 0), 0)
  checkpoint_save("page", 7)
  checkpoint_save("done", c("a.csv", "b.csv"))

  # The block's folder as MageML and the services' worker name it.
  expect_equal(list.files(root), "folder__pages")
  folder <- file.path(root, "folder__pages")
  expect_equal(sort(list.files(folder)), c("done.json", "page.json"))
  expect_equal(readLines(file.path(folder, "page.json")), "7")
  # The next attempt is another R session: only the files carry the values.
  state$checkpoints <- NULL
  expect_equal(checkpoint_load("page", default = 0), 7)
  expect_equal(checkpoint_load("done"), c("a.csv", "b.csv"))
})

test_that("Python's checkpoint files are read as R values", {
  root <- withr::local_tempdir()
  local_mageml_state(
    globals = list(checkpoint_root = root),
    context = list(block_uuid = "pages")
  )
  dir.create(file.path(root, "pages"))
  writeLines(
    '{"cursor": "abc", "files": ["x.csv"]}',
    file.path(root, "pages", "state.json")
  )

  expect_equal(
    checkpoint_load("state"),
    list(cursor = "abc", files = "x.csv")
  )
})

test_that("outside a pipeline run checkpoints last for the execution", {
  local_mageml_state(context = list(block_uuid = "pages"))

  checkpoint_save("n", 1)

  expect_equal(checkpoint_load("n"), 1)
})

test_that("keys are file names and values are small JSON", {
  local_mageml_state(context = list(block_uuid = "pages"))

  for (key in list("", "../escape", ".hidden", "a/b", c("a", "b"), 1)) {
    expect_error(checkpoint_save(key, 1), "A checkpoint key")
  }
  expect_error(
    checkpoint_save("big", strrep("x", 1024^2)),
    "over 1 MB"
  )
  expect_error(checkpoint_save("env", new.env()), "takes a value JSON")
})
