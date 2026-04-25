-- name: length_empty_string
-- description: LENGTH of empty string is 0

SELECT LENGTH('');
-- result:
-- rows: 1
-- [0][0]: 0
