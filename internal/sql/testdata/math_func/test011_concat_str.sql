-- name: concat_str_numbers
-- description: CONCAT with numbers converts to string

SELECT CONCAT('Value: ', 42);
-- result:
-- rows: 1
-- [0][0]: Value: 42
