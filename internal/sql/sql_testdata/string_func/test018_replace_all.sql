-- name: replace_all
-- description: REPLACE substitutes all occurrences

SELECT REPLACE('aaa', 'a', 'b');
-- result:
-- rows: 1
-- [0][0]: bbb
