-- name: substring_with_length
-- description: SUBSTRING extracts part of string with length

SELECT SUBSTRING('Hello World' FROM 1 FOR 5);
-- result:
-- rows: 1
-- [0][0]: Hello
