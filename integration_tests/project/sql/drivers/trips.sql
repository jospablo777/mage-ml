SELECT driver_id, trips, accepted, offered
FROM {schema}.trips
WHERE driver_id > %(last_id)s AND driver_id <= %(high_id)s
ORDER BY driver_id
