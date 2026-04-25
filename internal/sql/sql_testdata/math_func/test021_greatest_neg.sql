-- name: greatest_negative
-- description: GREATEST returns largest among negative numbers

SELECT GREATEST(-5, -10, -3);
-- result:
-- rows: 1
-- [0][0]: -3