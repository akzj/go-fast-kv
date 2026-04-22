package internal

import (
	"testing"

	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

// ─── CREATE VIEW Tests ────────────────────────────────────────────

func TestParse_CreateView(t *testing.T) {
	p := New()

	t.Run("basic view", func(t *testing.T) {
		stmt, err := p.Parse("CREATE VIEW v AS SELECT id, name FROM users")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		cv, ok := stmt.(*api.CreateViewStmt)
		if !ok {
			t.Fatalf("expected CreateViewStmt, got %T", stmt)
		}
		// Parser uppercases identifiers
		if cv.Name != "V" {
			t.Errorf("name = %q, want %q", cv.Name, "V")
		}
		if cv.QuerySQL == "" {
			t.Error("QuerySQL should not be empty")
		}
		if cv.Select == nil {
			t.Error("Select should not be nil")
		}
		sel, ok := cv.Select.(*api.SelectStmt)
		if !ok {
			t.Fatalf("expected SelectStmt, got %T", cv.Select)
		}
		if len(sel.Columns) != 2 {
			t.Errorf("columns count = %d, want 2", len(sel.Columns))
		}
	})

	t.Run("error: missing AS", func(t *testing.T) {
		_, err := p.Parse("CREATE VIEW v SELECT * FROM t")
		if err == nil {
			t.Error("expected error for missing AS")
		}
	})

	t.Run("error: missing SELECT", func(t *testing.T) {
		_, err := p.Parse("CREATE VIEW v AS")
		if err == nil {
			t.Error("expected error for missing SELECT")
		}
	})

	t.Run("view with subquery", func(t *testing.T) {
		stmt, err := p.Parse("CREATE VIEW v AS SELECT * FROM (SELECT id FROM users) AS subq")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		cv, ok := stmt.(*api.CreateViewStmt)
		if !ok {
			t.Fatalf("expected CreateViewStmt, got %T", stmt)
		}
		if cv.Select == nil {
			t.Error("Select should not be nil")
		}
	})

	t.Run("view with WHERE clause", func(t *testing.T) {
		stmt, err := p.Parse("CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		cv, ok := stmt.(*api.CreateViewStmt)
		if !ok {
			t.Fatalf("expected CreateViewStmt, got %T", stmt)
		}
		sel, ok := cv.Select.(*api.SelectStmt)
		if !ok || sel.Where == nil {
			t.Error("expected WHERE clause in view")
		}
	})

	t.Run("view with GROUP BY", func(t *testing.T) {
		stmt, err := p.Parse("CREATE VIEW stat AS SELECT dept, COUNT(*) FROM employees GROUP BY dept")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		cv, ok := stmt.(*api.CreateViewStmt)
		if !ok {
			t.Fatalf("expected CreateViewStmt, got %T", stmt)
		}
		sel, ok := cv.Select.(*api.SelectStmt)
		if !ok || sel.GroupBy == nil {
			t.Error("expected GROUP BY in view")
		}
	})

	t.Run("view with ORDER BY", func(t *testing.T) {
		stmt, err := p.Parse("CREATE VIEW sorted AS SELECT id FROM t ORDER BY id DESC")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		cv, ok := stmt.(*api.CreateViewStmt)
		if !ok {
			t.Fatalf("expected CreateViewStmt, got %T", stmt)
		}
		sel, ok := cv.Select.(*api.SelectStmt)
		if !ok || sel.OrderBy == nil {
			t.Error("expected ORDER BY in view")
		}
	})

	t.Run("view with LIMIT", func(t *testing.T) {
		stmt, err := p.Parse("CREATE VIEW top10 AS SELECT id FROM t ORDER BY score DESC LIMIT 10")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateViewStmt)
		if !ok {
			t.Fatalf("expected CreateViewStmt, got %T", stmt)
		}
	})
}

// ─── DROP VIEW Tests ─────────────────────────────────────────────

func TestParse_DropView(t *testing.T) {
	p := New()

	t.Run("basic drop view", func(t *testing.T) {
		stmt, err := p.Parse("DROP VIEW v")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		dv, ok := stmt.(*api.DropViewStmt)
		if !ok {
			t.Fatalf("expected DropViewStmt, got %T", stmt)
		}
		if dv.Name != "V" {
			t.Errorf("name = %q, want %q", dv.Name, "V")
		}
		if dv.IfExists {
			t.Error("IfExists should be false")
		}
	})

	t.Run("drop view IF EXISTS", func(t *testing.T) {
		stmt, err := p.Parse("DROP VIEW IF EXISTS v")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		dv, ok := stmt.(*api.DropViewStmt)
		if !ok {
			t.Fatalf("expected DropViewStmt, got %T", stmt)
		}
		if dv.Name != "V" {
			t.Errorf("name = %q, want %q", dv.Name, "V")
		}
		if !dv.IfExists {
			t.Error("IfExists should be true")
		}
	})

	t.Run("error: missing view name", func(t *testing.T) {
		_, err := p.Parse("DROP VIEW")
		if err == nil {
			t.Error("expected error for missing view name")
		}
	})

	t.Run("error: missing view name with IF EXISTS", func(t *testing.T) {
		_, err := p.Parse("DROP VIEW IF EXISTS")
		if err == nil {
			t.Error("expected error for missing view name")
		}
	})
}

// ─── CREATE TRIGGER Tests ─────────────────────────────────────────

func TestParse_CreateTrigger(t *testing.T) {
	p := New()

	t.Run("basic AFTER INSERT trigger", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TRIGGER my_trigger AFTER INSERT ON users BEGIN SELECT 1; END")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		tr, ok := stmt.(*api.TriggerStmt)
		if !ok {
			t.Fatalf("expected TriggerStmt, got %T", stmt)
		}
		if tr.Name != "MY_TRIGGER" {
			t.Errorf("name = %q, want %q", tr.Name, "MY_TRIGGER")
		}
		if tr.Timing != "AFTER" {
			t.Errorf("timing = %q, want %q", tr.Timing, "AFTER")
		}
		if tr.Body == nil {
			t.Error("Body should not be nil")
		}
	})

	t.Run("BEFORE DELETE trigger", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TRIGGER del_log BEFORE DELETE ON orders BEGIN DELETE FROM log WHERE ref = 'order'; END")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		tr, ok := stmt.(*api.TriggerStmt)
		if !ok {
			t.Fatalf("expected TriggerStmt, got %T", stmt)
		}
		if tr.Timing != "BEFORE" {
			t.Errorf("timing = %q, want %q", tr.Timing, "BEFORE")
		}
	})

	t.Run("AFTER UPDATE trigger", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TRIGGER upd_ts AFTER UPDATE ON products BEGIN UPDATE products SET ts = CURRENT_TIMESTAMP WHERE id = NEW.id; END")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		tr, ok := stmt.(*api.TriggerStmt)
		if !ok {
			t.Fatalf("expected TriggerStmt, got %T", stmt)
		}
		_ = tr
	})

	t.Run("INSTEAD OF trigger on view", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TRIGGER v_ins INSTEAD OF INSERT ON my_view BEGIN INSERT INTO t VALUES (NEW.a, NEW.b); END")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		tr, ok := stmt.(*api.TriggerStmt)
		if !ok {
			t.Fatalf("expected TriggerStmt, got %T", stmt)
		}
		if tr.Timing != "INSTEAD OF" {
			t.Errorf("timing = %q, want %q", tr.Timing, "INSTEAD OF")
		}
	})

	t.Run("trigger with WHEN clause", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TRIGGER check_val AFTER INSERT ON t WHEN NEW.val > 0 BEGIN SELECT 1; END")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		tr, ok := stmt.(*api.TriggerStmt)
		if !ok {
			t.Fatalf("expected TriggerStmt, got %T", stmt)
		}
		_ = tr
	})

	t.Run("trigger with multiple statements in body", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TRIGGER multi AFTER INSERT ON t BEGIN INSERT INTO log(msg) VALUES ('insert'); UPDATE stats SET cnt = cnt + 1; END")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		tr, ok := stmt.(*api.TriggerStmt)
		if !ok {
			t.Fatalf("expected TriggerStmt, got %T", stmt)
		}
		_ = tr
	})

	t.Run("error: missing trigger name", func(t *testing.T) {
		_, err := p.Parse("CREATE TRIGGER AFTER INSERT ON t BEGIN END")
		if err == nil {
			t.Error("expected error for missing trigger name")
		}
	})

	t.Run("error: missing END", func(t *testing.T) {
		_, err := p.Parse("CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1")
		if err == nil {
			t.Error("expected error for missing END")
		}
	})

	t.Run("error: missing table name", func(t *testing.T) {
		_, err := p.Parse("CREATE TRIGGER t AFTER INSERT BEGIN SELECT 1; END")
		if err == nil {
			t.Error("expected error for missing table name")
		}
	})
}

// ─── DROP TRIGGER Tests ──────────────────────────────────────────

func TestParse_DropTrigger(t *testing.T) {
	p := New()

	t.Run("basic drop trigger", func(t *testing.T) {
		stmt, err := p.Parse("DROP TRIGGER my_trigger")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		dt, ok := stmt.(*api.DropTriggerStmt)
		if !ok {
			t.Fatalf("expected DropTriggerStmt, got %T", stmt)
		}
		if dt.Name != "MY_TRIGGER" {
			t.Errorf("name = %q, want %q", dt.Name, "MY_TRIGGER")
		}
		if dt.IfExists {
			t.Error("IfExists should be false")
		}
	})

	t.Run("drop trigger IF EXISTS", func(t *testing.T) {
		stmt, err := p.Parse("DROP TRIGGER IF EXISTS old_trigger")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		dt, ok := stmt.(*api.DropTriggerStmt)
		if !ok {
			t.Fatalf("expected DropTriggerStmt, got %T", stmt)
		}
		if !dt.IfExists {
			t.Error("IfExists should be true")
		}
	})

	t.Run("error: missing trigger name", func(t *testing.T) {
		_, err := p.Parse("DROP TRIGGER")
		if err == nil {
			t.Error("expected error for missing trigger name")
		}
	})

	t.Run("error: missing trigger name with IF EXISTS", func(t *testing.T) {
		_, err := p.Parse("DROP TRIGGER IF EXISTS")
		if err == nil {
			t.Error("expected error for missing trigger name")
		}
	})
}

// ─── ON CONFLICT Clause Tests ─────────────────────────────────────

func TestParse_OnConflictClause(t *testing.T) {
	p := New()

	t.Run("ON CONFLICT DO NOTHING", func(t *testing.T) {
		stmt, err := p.Parse("INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT DO NOTHING")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
		if ins.OnConflict == nil {
			t.Fatal("OnConflict should not be nil")
		}
		if ins.OnConflict.Action != api.ConflictDoNothing {
			t.Errorf("action = %v, want %v", ins.OnConflict.Action, api.ConflictDoNothing)
		}
	})

	t.Run("ON CONFLICT with column DO NOTHING", func(t *testing.T) {
		stmt, err := p.Parse("INSERT INTO users (id) VALUES (1) ON CONFLICT(ID) DO NOTHING")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
		if len(ins.OnConflict.ConflictColumns) != 1 || ins.OnConflict.ConflictColumns[0] != "ID" {
			t.Errorf("ConflictColumns = %v, want [ID]", ins.OnConflict.ConflictColumns)
		}
	})

	t.Run("ON CONFLICT DO UPDATE SET", func(t *testing.T) {
		stmt, err := p.Parse("INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT(ID) DO UPDATE SET name = 'updated'")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
		if ins.OnConflict == nil {
			t.Fatal("OnConflict should not be nil")
		}
	})

	t.Run("ON CONFLICT multi-column", func(t *testing.T) {
		stmt, err := p.Parse("INSERT INTO orders (user_id, product_id) VALUES (1, 2) ON CONFLICT(USER_ID, PRODUCT_ID) DO NOTHING")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
		if len(ins.OnConflict.ConflictColumns) != 2 {
			t.Errorf("ConflictColumns count = %d, want 2", len(ins.OnConflict.ConflictColumns))
		}
	})

	t.Run("ON CONFLICT with multiple SET", func(t *testing.T) {
		stmt, err := p.Parse("INSERT INTO users (id, a, b) VALUES (1, 2, 3) ON CONFLICT(ID) DO UPDATE SET A = 10, B = 20")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
	})

	t.Run("error: ON CONFLICT without DO", func(t *testing.T) {
		_, err := p.Parse("INSERT INTO users VALUES (1) ON CONFLICT")
		if err == nil {
			t.Error("expected error for ON CONFLICT without DO")
		}
	})

	t.Run("error: ON CONFLICT without column name", func(t *testing.T) {
		_, err := p.Parse("INSERT INTO users VALUES (1) ON CONFLICT() DO NOTHING")
		if err == nil {
			t.Error("expected error for ON CONFLICT with empty column list")
		}
	})
}

// ─── Create Table Extended Tests ────────────────────────────────

func TestParse_CreateTable_Extended(t *testing.T) {
	p := New()

	t.Run("table with multiple columns", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (id INT, name TEXT, price FLOAT, qty INT DEFAULT 0, created_at TEXT)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
		if len(ct.Columns) < 5 {
			t.Errorf("expected 5+ columns, got %d", len(ct.Columns))
		}
		_ = ct
	})

	t.Run("table with single column", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (x INT)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
		if len(ct.Columns) != 1 {
			t.Errorf("expected 1 column, got %d", len(ct.Columns))
		}
		_ = ct
	})

	t.Run("table with default values", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (name TEXT DEFAULT 'anon', age INT DEFAULT 0)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
		_ = ct
	})

	t.Run("table with NOT NULL constraint", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (name TEXT NOT NULL)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
	})

	t.Run("table with UNIQUE constraint", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (email TEXT UNIQUE)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
	})

	t.Run("table with CHECK constraint", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (age INT CHECK (age >= 0))")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
	})

	t.Run("table with PRIMARY KEY", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (id INT PRIMARY KEY)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
	})

	t.Run("table with AUTOINCREMENT", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
	})
}

// ─── Insert Extended Tests ────────────────────────────────────────

func TestParse_Insert_Extended(t *testing.T) {
	p := New()

	t.Run("INSERT with SELECT", func(t *testing.T) {
		stmt, err := p.Parse("INSERT INTO new SELECT * FROM old")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
		if ins.SelectStmt == nil {
			t.Error("expected SELECT in INSERT")
		}
	})

	t.Run("INSERT single row", func(t *testing.T) {
		stmt, err := p.Parse("INSERT INTO t VALUES (1, 'hello')")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
	})

	t.Run("INSERT with column list", func(t *testing.T) {
		stmt, err := p.Parse("INSERT INTO t (a, b, c) VALUES (1, 2, 3)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
		if len(ins.Columns) != 3 {
			t.Errorf("expected 3 columns, got %d", len(ins.Columns))
		}
	})

	t.Run("INSERT multiple rows", func(t *testing.T) {
		stmt, err := p.Parse("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ins, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
		if len(ins.Values) != 3 {
			t.Errorf("expected 3 rows, got %d", len(ins.Values))
		}
	})

	t.Run("INSERT with NULL", func(t *testing.T) {
		stmt, err := p.Parse("INSERT INTO t VALUES (1, NULL)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.InsertStmt)
		if !ok {
			t.Fatalf("expected InsertStmt, got %T", stmt)
		}
	})
}

// ─── FactorExpr Extended Tests ───────────────────────────────────

func TestParse_FactorExpr_Extended(t *testing.T) {
	p := New()

	t.Run("division operator", func(t *testing.T) {
		stmt, err := p.Parse("SELECT 10 / 3")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) == 0 {
			t.Fatalf("no columns parsed")
		}
		_ = sel.Columns[0].Expr
	})

	t.Run("modulo operator", func(t *testing.T) {
		stmt, err := p.Parse("SELECT 10 % 3")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) == 0 {
			t.Fatalf("no columns parsed")
		}
	})

	t.Run("complex arithmetic", func(t *testing.T) {
		stmt, err := p.Parse("SELECT (a + b) / (c - d)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Columns[0].Expr == nil {
			t.Error("expression should not be nil")
		}
	})

	t.Run("division with column", func(t *testing.T) {
		stmt, err := p.Parse("SELECT a / b")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Columns[0].Expr == nil {
			t.Error("expression should not be nil")
		}
	})

	t.Run("division with zero", func(t *testing.T) {
		stmt, err := p.Parse("SELECT a / 0")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Columns[0].Expr == nil {
			t.Error("expression should not be nil")
		}
	})
}

// ─── Subquery Select Extended Tests ─────────────────────────────

func TestParse_SubquerySelect_Extended(t *testing.T) {
	p := New()

	t.Run("subquery in WHERE IN", func(t *testing.T) {
		stmt, err := p.Parse("SELECT * FROM t WHERE id IN (SELECT id FROM other)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Where == nil {
			t.Error("WHERE should not be nil")
		}
	})

	t.Run("subquery in FROM", func(t *testing.T) {
		stmt, err := p.Parse("SELECT * FROM (SELECT id FROM t) AS subq")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.DerivedTable == nil {
			t.Error("DerivedTable should not be nil for subquery")
		}
	})

	t.Run("subquery as column", func(t *testing.T) {
		stmt, err := p.Parse("SELECT (SELECT max(id) FROM t) AS max_id")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) == 0 {
			t.Error("expected columns")
		}
	})

	t.Run("scalar subquery with LIMIT", func(t *testing.T) {
		stmt, err := p.Parse("SELECT * FROM t WHERE id = (SELECT id FROM o LIMIT 1)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Where == nil {
			t.Error("WHERE should not be nil")
		}
	})

	t.Run("subquery with ORDER BY", func(t *testing.T) {
		stmt, err := p.Parse("SELECT * FROM t WHERE id IN (SELECT id FROM x ORDER BY id)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Where == nil {
			t.Error("WHERE should not be nil")
		}
	})

	t.Run("subquery with LIMIT", func(t *testing.T) {
		stmt, err := p.Parse("SELECT * FROM t WHERE id IN (SELECT id FROM x LIMIT 5)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if sel.Where == nil {
			t.Error("WHERE should not be nil")
		}
	})
}

// ─── Window Function Extended Tests ─────────────────────────────

func TestParse_WindowFunction_Extended(t *testing.T) {
	p := New()

	t.Run("LAG function", func(t *testing.T) {
		stmt, err := p.Parse("SELECT LAG(val) OVER (ORDER BY id) FROM t")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) == 0 {
			t.Error("expected columns")
		}
	})

	t.Run("LEAD function", func(t *testing.T) {
		stmt, err := p.Parse("SELECT LEAD(val) OVER (ORDER BY id) FROM t")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) == 0 {
			t.Error("expected columns")
		}
	})

	t.Run("FIRST_VALUE function", func(t *testing.T) {
		stmt, err := p.Parse("SELECT FIRST_VALUE(x) OVER (PARTITION BY y ORDER BY z) FROM t")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) == 0 {
			t.Error("expected columns")
		}
	})

	t.Run("LAST_VALUE function", func(t *testing.T) {
		stmt, err := p.Parse("SELECT LAST_VALUE(x) OVER (PARTITION BY y ORDER BY z) FROM t")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) == 0 {
			t.Error("expected columns")
		}
	})

	t.Run("COUNT function", func(t *testing.T) {
		stmt, err := p.Parse("SELECT COUNT(*) OVER (ORDER BY y) FROM t")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		sel := stmt.(*api.SelectStmt)
		if len(sel.Columns) == 0 {
			t.Error("expected columns")
		}
	})
}

// ─── Error Recovery Tests ────────────────────────────────────────

func TestParse_ErrorRecovery(t *testing.T) {
	p := New()

	t.Run("error in CREATE VIEW", func(t *testing.T) {
		_, err := p.Parse("CREATE VIEW v")
		if err == nil {
			t.Error("expected error for incomplete CREATE VIEW")
		}
	})

	t.Run("error in CREATE TRIGGER", func(t *testing.T) {
		_, err := p.Parse("CREATE TRIGGER t")
		if err == nil {
			t.Error("expected error for incomplete CREATE TRIGGER")
		}
	})

	t.Run("error in ON CONFLICT", func(t *testing.T) {
		_, err := p.Parse("INSERT INTO t VALUES (1) ON CONFLICT")
		if err == nil {
			t.Error("expected error for incomplete ON CONFLICT")
		}
	})

	t.Run("error: trailing garbage", func(t *testing.T) {
		_, err := p.Parse("SELECT 1 extra garbage")
		if err == nil {
			t.Error("expected error for trailing garbage")
		}
	})

	t.Run("error: incomplete statement", func(t *testing.T) {
		_, err := p.Parse("SELECT * FROM")
		if err == nil {
			t.Error("expected error for incomplete statement")
		}
	})

	t.Run("error: unmatched parenthesis", func(t *testing.T) {
		_, err := p.Parse("SELECT * FROM t WHERE id IN (SELECT id")
		if err == nil {
			t.Error("expected error for unmatched parenthesis")
		}
	})
}

// ─── ColumnDef Extended Tests ───────────────────────────────────

func TestParse_ColumnDef_Extended(t *testing.T) {
	p := New()

	t.Run("primary key constraint", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (id INT PRIMARY KEY)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
		if len(ct.Columns) == 0 {
			t.Fatal("no columns parsed")
		}
		_ = ct
	})

	t.Run("unique constraint", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (email TEXT UNIQUE)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
	})

	t.Run("not null constraint", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (name TEXT NOT NULL)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
	})

	t.Run("default expression", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
	})

	t.Run("check constraint", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (age INT CHECK (age >= 0))")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
	})

	t.Run("primary key with autoincrement", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
	})

	t.Run("multiple columns", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (a INT, b TEXT, c FLOAT)")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
		if len(ct.Columns) != 3 {
			t.Errorf("expected 3 columns, got %d", len(ct.Columns))
		}
		_ = ct
	})

	t.Run("table with table-level PRIMARY KEY", func(t *testing.T) {
		stmt, err := p.Parse("CREATE TABLE t (a INT, b TEXT, PRIMARY KEY (a))")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		ct, ok := stmt.(*api.CreateTableStmt)
		if !ok {
			t.Fatalf("expected CreateTableStmt, got %T", stmt)
		}
		_ = ct
	})
}