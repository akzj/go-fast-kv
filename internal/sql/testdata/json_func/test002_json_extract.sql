-- name: json_extract
-- description: JSON_EXTRACT extracts value from JSON

SELECT JSON_EXTRACT('{"name":"Alice"}', '$.name');
-- result:
-- rows: 1
-- [0][0]: "Alice"
