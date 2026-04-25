-- name: jsonb_extract_path
-- description: JSONB_EXTRACT_PATH extracts path from JSON

SELECT JSONB_EXTRACT_PATH_TEXT('{"name":"Bob"}', 'name');
-- result:
-- rows: 1
-- [0][0]: Bob
