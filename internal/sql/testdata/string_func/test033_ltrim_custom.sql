-- name: ltrim_no_spaces
-- description: LTRIM with no spaces

SELECT LTRIM('hello');
-- result:
-- rows: 1
-- [0][0]: hello
