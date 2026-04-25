-- name: split_part_basic
-- description: SPLIT_PART splits string by delimiter

SELECT SPLIT_PART('a|b|c', '|', 2);
-- result:
-- rows: 1
-- [0][0]: b
