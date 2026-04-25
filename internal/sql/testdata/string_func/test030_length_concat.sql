-- name: length_concat
-- description: LENGTH of CONCAT result

SELECT LENGTH(CONCAT('he', 'llo'));
-- result:
-- rows: 1
-- [0][0]: 5
