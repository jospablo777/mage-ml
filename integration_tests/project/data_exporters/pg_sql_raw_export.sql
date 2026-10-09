INSERT INTO {{ variables('schema') }}.pg_sql_raw_dst (id, c_big, c_text, c_date, c_tstz, c_int_list, c_struct)
SELECT id, c_big, c_text, c_date, c_tstz, c_int_list, c_struct FROM {{ df_1 }};
