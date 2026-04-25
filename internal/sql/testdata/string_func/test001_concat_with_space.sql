-- name: concat_with_space
-- description: CONCAT joins strings with explicit space argument

SELECT CONCAT('Hello', ' ', 'World');
-- result:
-- rows: 1
-- [0][0]: Hello World
