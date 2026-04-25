-- name: ltrim_basic
-- description: LTRIM removes leading whitespace

SELECT LTRIM('  hello');
-- result:
-- rows: 1
-- [0][0]: hello
