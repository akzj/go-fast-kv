-- name: split_part_third
-- description: SPLIT_PART extracts third element

SELECT SPLIT_PART('a.b.c', '.', 3);
-- result:
-- rows: 1
-- [0][0]: c
