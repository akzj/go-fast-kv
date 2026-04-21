package internal

import (
	"testing"
)

func TestFKParsing(t *testing.T) {
	p := New()

	tests := []struct {
		sql    string
		expect string
	}{
		{
			`CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))`,
			"table-level FK",
		},
		{
			`CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)`,
			"FK with ON DELETE CASCADE",
		},
		{
			`CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE SET NULL)`,
			"FK with ON UPDATE SET NULL",
		},
		{
			`CREATE TABLE t (a INT, b INT, FOREIGN KEY (a, b) REFERENCES p(x, y))`,
			"multi-column FK",
		},
	}

	for _, tc := range tests {
		stmt, err := p.Parse(tc.sql)
		if err != nil {
			t.Errorf("FAIL %s: %v", tc.expect, err)
		} else {
			t.Logf("PASS %s: %T", tc.expect, stmt)
		}
	}
}
