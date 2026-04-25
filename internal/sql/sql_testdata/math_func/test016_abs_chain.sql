-- name: abs_chain
-- description: ABS can be chained with other operations

SELECT ABS(-10) + 5;
-- result:
-- rows: 1
-- [0][0]: 15
