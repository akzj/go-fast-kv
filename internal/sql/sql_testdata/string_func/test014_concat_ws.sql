-- name: concat_ws_basic
-- description: CONCAT_WS joins with separator, ignoring NULLs

SELECT CONCAT_WS('-', 'a', 'b', 'c');
-- result:
-- rows: 1
-- [0][0]: a-b-c
