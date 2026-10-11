suppressPackageStartupMessages(library(tidyverse))

# Scores each row with a model saved in R with saveRDS(), such as one fitted
# with lm(), glm(), ranger or a tidymodels workflow. Set its path in the project
# with the model variable. predict() returns a vector for most models and a
# tibble for tidymodels: both become columns of the output.
#* @transformer
score <- function(df_1, ...) {
  model <- readRDS(variable("model", default = "models/model.rds"))
  predictions <- predict(model, df_1)
  if (is.data.frame(predictions)) {
    bind_cols(df_1, predictions)
  } else {
    mutate(df_1, prediction = as.vector(predictions))
  }
}

#* @test
every_row_is_scored <- function(output) {
  stopifnot("The output is empty" = nrow(output) > 0)
}
