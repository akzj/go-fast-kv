-- name: jsonb_extract_path
-- description: JSONB_EXTRACT_PATH extracts value at path

SELECT JSONB_EXTRACT_PATH('{"a":{"b":1}}', 'a', 'b');
-- result:
-- rows: 1
-- [0][0]: 1
