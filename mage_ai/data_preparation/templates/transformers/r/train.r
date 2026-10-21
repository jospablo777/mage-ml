suppressPackageStartupMessages({
  library(tidyverse)
  library(tidymodels)
})

# Trains a random forest (ranger) that predicts the column the target variable
# names (target by default) from the other columns, on 80% of the rows, and
# saves it with saveRDS to the path of the model variable (models/model.rds by
# default), where the Model inference template reads it. A text or factor
# target makes it a classifier. Returns its metrics on the other 20%. It needs
# tidymodels and ranger, which `mage r init` leaves out: add them with
# `rv add tidymodels ranger`.
#* @transformer
train <- function(df_1, ...) {
  target <- variable("target", default = "target")
  path <- variable("model", default = "models/model.rds")
  rows <- mutate(df_1, across(where(is.character), as.factor))
  classes <- is.factor(rows[[target]])
  set.seed(42)
  split <- initial_split(rows, prop = 0.8)
  model <- rand_forest(trees = 200) |>
    set_engine("ranger") |>
    set_mode(if (classes) "classification" else "regression") |>
    fit(as.formula(str_c(target, " ~ .")), data = training(split))
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  saveRDS(model, path)
  estimate <- if (classes) ".pred_class" else ".pred"
  predict(model, testing(split)) |>
    bind_cols(testing(split)) |>
    metrics(truth = !!sym(target), estimate = !!sym(estimate))
}

#* @test
the_model_was_saved <- function(output) {
  path <- variable("model", default = "models/model.rds")
  stopifnot("The model was not saved" = file.exists(path))
}
