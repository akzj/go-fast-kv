-- name: jsonb_build_array
-- description: JSONB_BUILD_ARRAY builds array from values

SELECT JSONB_BUILD_ARRAY(1, 2, 3);
-- result:
-- rows: 1
