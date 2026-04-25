-- name: date_trunc_basic
-- description: DATE_TRUNC truncates timestamp to unit

SELECT DATE_TRUNC('day', NOW());
-- result:
-- rows: 1
