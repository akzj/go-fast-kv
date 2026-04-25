-- name: concat_ws_multi
-- description: CONCAT_WS with multiple separators

SELECT CONCAT_WS(',', 'a', 'b', 'c', 'd');
-- result:
-- rows: 1
-- [0][0]: a,b,c,d
