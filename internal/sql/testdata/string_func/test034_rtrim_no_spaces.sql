-- name: rtrim_no_spaces
-- description: RTRIM with no spaces

SELECT RTRIM('hello');
-- result:
-- rows: 1
-- [0][0]: hello
