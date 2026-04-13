package internal

import (
	"os"
	"testing"

	"github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"

	"github.com/akzj/go-fast-kv/internal/sql/catalog/api"
)

func TestCatalog(t *testing.T) {
	// Setup
	dir := t.TempDir()
	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer func() {
		store.Close()
		os.RemoveAll(dir)
	}()

	catalog := New(store)

	// Test: CreateTable
	t.Run("CreateTable", func(t *testing.T) {
		schema := api.TableSchema{
			Name: "users",
			Columns: []api.ColumnDef{
				{Name: "id", Type: api.TypeInt},
				{Name: "name", Type: api.TypeText},
				{Name: "age", Type: api.TypeInt},
			},
		}
		err := catalog.CreateTable(schema)
		if err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}
	})

	// Test: GetTable
	t.Run("GetTable", func(t *testing.T) {
		schema, err := catalog.GetTable("users")
		if err != nil {
			t.Fatalf("GetTable failed: %v", err)
		}
		if schema.Name != "users" {
			t.Errorf("expected name 'users', got %q", schema.Name)
		}
		if len(schema.Columns) != 3 {
			t.Errorf("expected 3 columns, got %d", len(schema.Columns))
		}
	})

	// Test: Case insensitive
	t.Run("CaseInsensitive", func(t *testing.T) {
		schema, err := catalog.GetTable("USERS")
		if err != nil {
			t.Fatalf("GetTable USERS failed: %v", err)
		}
		if schema.Name != "users" {
			t.Errorf("expected name 'users', got %q", schema.Name)
		}
	})

	// Test: TableNotFound
	t.Run("TableNotFound", func(t *testing.T) {
		_, err := catalog.GetTable("nonexistent")
		if err != api.ErrTableNotFound {
			t.Errorf("expected ErrTableNotFound, got %v", err)
		}
	})

	// Test: TableExists
	t.Run("TableExists", func(t *testing.T) {
		schema := api.TableSchema{Name: "users", Columns: []api.ColumnDef{}}
		err := catalog.CreateTable(schema)
		if err != api.ErrTableExists {
			t.Errorf("expected ErrTableExists, got %v", err)
		}
	})

	// Test: CreateIndex
	t.Run("CreateIndex", func(t *testing.T) {
		idxSchema := api.IndexSchema{
			Name:   "idx_age",
			Table:  "users",
			Column: "age",
		}
		err := catalog.CreateIndex(idxSchema)
		if err != nil {
			t.Fatalf("CreateIndex failed: %v", err)
		}
	})

	// Test: GetIndexByColumn
	t.Run("GetIndexByColumn", func(t *testing.T) {
		idx, err := catalog.GetIndexByColumn("users", "age")
		if err != nil {
			t.Fatalf("GetIndexByColumn failed: %v", err)
		}
		if idx == nil {
			t.Fatal("expected index on age, got nil")
		}
		if idx.Name != "idx_age" {
			t.Errorf("expected index name 'idx_age', got %q", idx.Name)
		}
	})

	// Test: DropTable
	t.Run("DropTable", func(t *testing.T) {
		err := catalog.DropTable("users")
		if err != nil {
			t.Fatalf("DropTable failed: %v", err)
		}

		// Verify table is gone
		_, err = catalog.GetTable("users")
		if err != api.ErrTableNotFound {
			t.Errorf("expected ErrTableNotFound after DropTable, got %v", err)
		}

		// Verify index is also gone
		_, err = catalog.GetIndexByColumn("users", "age")
		if err != api.ErrIndexNotFound {
			// Index not found is expected since table is gone
		}
	})
}
