#* @data_exporter
export_rows <- function(df_1, ...) {
  write_table(
    df_1,
    "r_dbi_dst",
    database = "postgres",
    profile = "test_schema",
    if_exists = "replace"
  )
}
