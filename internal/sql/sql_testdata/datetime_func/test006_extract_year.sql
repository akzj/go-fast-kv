-- name: extract_year
-- description: EXTRACT returns year from timestamp

SELECT EXTRACT(YEAR FROM NOW());
-- result:
-- rows: 1
