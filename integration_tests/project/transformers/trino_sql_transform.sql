SELECT
    *,
    big + 1 AS big_plus_one,
    length(text) AS text_length,
    year(day) AS day_year,
    CAST(at AS varchar) AS at_text,
    amount * 100 AS cents,
    CAST(key AS varchar) AS key_text
FROM {{ df_1 }}
