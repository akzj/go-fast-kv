-- name: upper_lower_chain
-- description: UPPER and LOWER can be chained

SELECT UPPER(LOWER('Hello'));
-- result:
-- rows: 1
-- [0][0]: HELLO