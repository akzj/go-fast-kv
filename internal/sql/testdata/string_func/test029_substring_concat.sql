-- name: substring_concat
-- description: SUBSTRING on CONCAT result

SELECT SUBSTRING(CONCAT('Hello', 'World') FROM 1 FOR 5);
-- result:
-- rows: 1
-- [0][0]: Hello
