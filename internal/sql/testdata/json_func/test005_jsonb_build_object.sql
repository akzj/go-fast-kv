-- name: jsonb_build_object
-- description: JSONB_BUILD_OBJECT creates JSON object

SELECT JSONB_BUILD_OBJECT('name', 'Alice');
-- result:
-- rows: 1
-- [0][0]: {"name":"Alice"}
