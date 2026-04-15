package internal

import (
    "testing"
    "fmt"
)

func TestCorrelatedDebug(t *testing.T) {
    env := newTestEnv(t)
    
    env.execSQL(t, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    env.execSQL(t, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
    
    env.execSQL(t, "INSERT INTO users VALUES (1, 'alice')")
    env.execSQL(t, "INSERT INTO users VALUES (2, 'bob')")
    env.execSQL(t, "INSERT INTO users VALUES (3, 'carol')")
    env.execSQL(t, "INSERT INTO users VALUES (4, 'dave')")
    
    env.execSQL(t, "INSERT INTO orders VALUES (1, 1, 100)")
    env.execSQL(t, "INSERT INTO orders VALUES (2, 1, 200)")
    env.execSQL(t, "INSERT INTO orders VALUES (3, 2, 150)")
    env.execSQL(t, "INSERT INTO orders VALUES (4, 4, 50)")
    env.execSQL(t, "INSERT INTO orders VALUES (5, 4, 75)")
    env.execSQL(t, "INSERT INTO orders VALUES (6, 4, 125)")
    
    // Direct query: what does the subquery return for each user?
    t.Log("=== Query: (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) ===")
    for _, userID := range []int64{1, 2, 3, 4} {
        q := fmt.Sprintf("SELECT COUNT(*) FROM orders WHERE orders.user_id = %d", userID)
        r := env.execSQL(t, q)
        t.Logf("  user_id=%d: COUNT=%d", userID, r.Rows[0][0].Int)
    }
    
    // The main query
    r := env.execSQL(t, "SELECT name FROM users WHERE (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) > 1")
    t.Logf("Main query returned %d rows:", len(r.Rows))
    for i, row := range r.Rows {
        t.Logf("  row[%d]: name=%s", i, row[0].Text)
    }
}
