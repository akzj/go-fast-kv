-- name: replace_literal
-- description: REPLACE substitutes substring

SELECT REPLACE('hello world', 'world', 'go-fast-kv');
-- result:
-- rows: 1
-- [0][0]: hello go-fast-kv
