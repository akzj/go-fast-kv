-- name: jsonb_build_object
-- description: JSONB_BUILD_OBJECT builds object from key-value pairs

SELECT JSONB_BUILD_OBJECT('name', 'Alice', 'age', 30);
-- result:
-- rows: 1
