-- name: jsonb_build_array_multi
-- description: JSONB_BUILD_ARRAY with multiple values

SELECT JSONB_BUILD_ARRAY(1, 2, 3);
-- result:
-- rows: 1
-- [0][0]: [1,2,3]
