-- name: concat_no_space
-- description: CONCAT joins strings without space

SELECT CONCAT('Hello', 'World');
-- result:
-- rows: 1
-- [0][0]: HelloWorld
