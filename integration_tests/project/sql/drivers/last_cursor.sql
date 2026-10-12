SELECT COALESCE(MAX(driver_id), 0) AS last_id FROM {schema}.driver_features
