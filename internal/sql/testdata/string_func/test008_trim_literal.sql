-- name: trim_literal
-- description: TRIM removes whitespace

SELECT TRIM('  hello  ');
-- result:
-- rows: 1
-- [0][0]: hello
