-- name: jsonb_build_array
-- description: JSONB_BUILD_ARRAY creates JSON array

SELECT JSONB_BUILD_ARRAY('a', 'b', 'c');
-- result:
-- rows: 1
-- [0][0]: ["a","b","c"]
