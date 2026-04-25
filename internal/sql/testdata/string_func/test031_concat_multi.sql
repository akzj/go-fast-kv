-- name: concat_multi_args
-- description: CONCAT with many arguments

SELECT CONCAT('a', 'b', 'c', 'd', 'e');
-- result:
-- rows: 1
-- [0][0]: abcde
