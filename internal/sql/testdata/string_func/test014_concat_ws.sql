-- name: concat_ws_basic
-- description: CONCAT_WS concatenates with separator

SELECT CONCAT_WS('-', 'a', 'b', 'c');
-- result:
-- rows: 1
-- [0][0]: a-b-c
