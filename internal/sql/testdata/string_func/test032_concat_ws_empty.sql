-- name: concat_ws_empty_sep
-- description: CONCAT_WS with empty separator

SELECT CONCAT_WS('', 'a', 'b');
-- result:
-- rows: 1
-- [0][0]: ab
