SELECT
    *,
    big + 1 AS big_plus_one,
    lengthUTF8(text) AS text_length,
    toYear(day) AS day_year,
    toUnixTimestamp64Micro(at) AS at_micros
FROM {{ df_1 }}
