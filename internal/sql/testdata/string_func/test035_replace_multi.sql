-- name: replace_multi_occurrence
-- description: REPLACE with multiple occurrences

SELECT REPLACE('aaa', 'a', 'b');
-- result:
-- rows: 1
-- [0][0]: bbb
