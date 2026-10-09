#* @data_loader
load_source <- function(...) {
  read_sql(
    "SELECT * FROM src WHERE id <= $1 ORDER BY id",
    database = "postgres",
    profile = "test_schema",
    params = list(variable("max_id"))
  )
}

#* @test
every_row_was_read <- function(output) {
  stopifnot(nrow(output) == variable("expected_rows"))
}
