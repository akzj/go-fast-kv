-- name: replace_multi_occurrence
-- description: REPLACE substitutes all occurrences

SELECT REPLACE('banana', 'a', 'o');
-- result:
-- rows: 1
-- [0][0]: bonono