SELECT
    *,
    c_int::bigint * 2 AS sql_int_times_two,
    char_length(c_text) AS sql_text_length,
    c_big::numeric + 1 AS sql_big_plus_one,
    cardinality(c_int_list) AS sql_list_length,
    c_ts + interval '1 hour' AS sql_ts_plus_hour,
    c_struct ->> 'b' AS sql_struct_b
FROM {{ df_1 }}
