#* @data_exporter
export_cohorts <- function(retention, ...) {
  write_table(
    retention,
    "r_cohort_retention",
    database = "postgres",
    profile = "default",
    schema = "analytics",
    if_exists = "replace"
  )
}
