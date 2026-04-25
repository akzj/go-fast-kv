-- name: rtrim_basic
-- description: RTRIM removes trailing spaces

SELECT RTRIM('hello   ');
-- result:
-- rows: 1
-- [0][0]: hello
