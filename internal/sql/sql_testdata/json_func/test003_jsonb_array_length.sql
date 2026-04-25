-- name: jsonb_array_length
-- description: JSONB_ARRAY_LENGTH returns array length

SELECT JSONB_ARRAY_LENGTH('[1,2,3]');
-- result:
-- rows: 1
-- [0][0]: 3
