-- name: ceil_basic
-- description: CEIL rounds up

SELECT CEIL(3.2);
-- result:
-- rows: 1
-- [0][0]: 4

SELECT CEIL(3.8);
-- result:
-- rows: 1
-- [0][0]: 4
