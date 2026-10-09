SELECT
    id, c_smallint, c_integer, c_bigint, c_numeric, c_double, c_bool, c_text, c_bytea,
    c_date, c_time, c_timestamp, c_timestamptz, c_interval, c_uuid, c_jsonb,
    c_int_array, c_text_array, c_enum
FROM {{ variables('schema') }}.src
