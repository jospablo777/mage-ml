SELECT
    *,
    length(c_varchar) AS text_length,
    c_bigint::HUGEINT + 1 AS bigint_plus_one,
    len(c_int_list) AS list_length,
    year(c_date) AS date_year
FROM {{ df_1 }}
