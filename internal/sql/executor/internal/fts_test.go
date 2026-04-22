package internal

import (
	"testing"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	catalogapi "github.com/akzj/go-fast-kv/internal/sql/catalog/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	"github.com/akzj/go-fast-kv/internal/sql/encoding"
	"github.com/akzj/go-fast-kv/internal/sql/engine"
	"github.com/akzj/go-fast-kv/internal/sql/parser"
	parserapi "github.com/akzj/go-fast-kv/internal/sql/parser/api"
	"github.com/akzj/go-fast-kv/internal/sql/planner"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
)

func TestFTS_ParseCreateTable(t *testing.T) {
	p := parser.New()

	stmt, err := p.Parse("CREATE VIRTUAL TABLE articles USING fts5(title, content)")
	if err != nil {
		t.Fatalf("parse CREATE VIRTUAL TABLE: %v", err)
	}

	fts, ok := stmt.(*parserapi.CreateFTSStmt)
	if !ok {
		t.Fatalf("expected CreateFTSStmt, got %T", stmt)
	}

	// Table names are uppercased
	if fts.Name != "ARTICLES" {
		t.Fatalf("expected table name 'ARTICLES', got %q", fts.Name)
	}
	if len(fts.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(fts.Columns))
	}
	if fts.FTSVersion != "fts5" {
		t.Fatalf("expected fts5, got %s", fts.FTSVersion)
	}
}

func TestFTS_ParseMatch(t *testing.T) {
	p := parser.New()

	stmt, err := p.Parse("SELECT * FROM articles WHERE articles MATCH 'database AND sql'")
	if err != nil {
		t.Fatalf("parse SELECT with MATCH: %v", err)
	}

	sel, ok := stmt.(*parserapi.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}

	match, ok := sel.Where.(*parserapi.MatchExpr)
	if !ok {
		t.Fatalf("expected MatchExpr, got %T", sel.Where)
	}

	// Table name is extracted from the left side of MATCH
	// Query is the FTS search string
	if match.Query != "database AND sql" {
		t.Fatalf("expected query 'database AND sql', got %q", match.Query)
	}
}

func TestFTS_Tokenize(t *testing.T) {
	store, err := kvstore.Open(kvstoreapi.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	fts := engine.NewFTSEngine(store)

	// Index a document
	err = fts.IndexDocument("test", 1, []string{"SQL Tutorial", "Learn SQL basics"}, "")
	if err != nil {
		t.Fatalf("IndexDocument: %v", err)
	}

	// Search for 'sql'
	docIDs, err := fts.Search("test", "sql")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(docIDs) != 1 {
		t.Fatalf("expected 1 docID, got %d", len(docIDs))
	}
	if docIDs[0] != 1 {
		t.Fatalf("expected docID 1, got %d", docIDs[0])
	}

	// Search for non-existent term
	docIDs, err = fts.Search("test", "nonexistent")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(docIDs) != 0 {
		t.Fatalf("expected 0 docIDs, got %d", len(docIDs))
	}
}

func TestFTS_DropTable(t *testing.T) {
	env := newTestEnvWithFTS(t)

	// Create FTS table
	_, err := env.exec.Execute(&plannerapi.CreateFTSPlan{
		Schema: plannerapi.FTSIndexSchema{
			Name:       "docs",
			Columns:    []string{"title"},
			FTSVersion: "fts5",
		},
	})
	if err != nil {
		t.Fatalf("CREATE FTS TABLE: %v", err)
	}

	// Drop table
	dropPlan := &plannerapi.DropTablePlan{
		TableName: "docs",
	}
	_, err = env.exec.Execute(dropPlan)
	if err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}

	// Verify table is gone
	_, err = env.cat.GetTable("docs")
	if err != catalogapi.ErrTableNotFound {
		t.Fatalf("expected ErrTableNotFound, got %v", err)
	}
}

func newTestEnvWithFTS(t *testing.T) *testEnv {
	t.Helper()
	store, err := kvstore.Open(kvstoreapi.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	cat := catalog.New(store)
	enc := encoding.NewKeyEncoder()
	codec := encoding.NewRowCodec()
	tbl := engine.NewTableEngine(store, enc, codec)
	idx := engine.NewIndexEngine(store, enc)
	fts := engine.NewFTSEngine(store)
	p := parser.New()
	pl := planner.New(cat)
	exec := New(store, cat, tbl, idx, fts, pl, p)

	return &testEnv{
		store:  store,
		cat:    cat,
		parser: p,
		planner: pl,
		exec:   exec,
		enc:    enc,
	}
}
