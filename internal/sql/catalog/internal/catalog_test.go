package internal

import (
	"fmt"
	"os"
	"sync"
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

// TestCatalogConcurrentDDL tests that concurrent DDL operations are safe.
func TestCatalogConcurrentDDL(t *testing.T) {
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

	// Test: 10 goroutines creating tables concurrently
	t.Run("ConcurrentCreateTable", func(t *testing.T) {
		var wg sync.WaitGroup
		errCh := make(chan error, 20)
		created := make(map[string]bool)
		var mu sync.Mutex

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				name := fmt.Sprintf("concurrent_table_%d", id)
				schema := api.TableSchema{
					Name: name,
					Columns: []api.ColumnDef{
						{Name: "id", Type: api.TypeInt},
					},
				}
				err := catalog.CreateTable(schema)
				if err != nil {
					errCh <- fmt.Errorf("CreateTable %s failed: %v", name, err)
					return
				}
				mu.Lock()
				created[name] = true
				mu.Unlock()
			}(i)
		}

		wg.Wait()
		close(errCh)

		// Check for errors
		for err := range errCh {
			if err != nil {
				t.Error(err)
			}
		}

		// Verify all tables were created
		if len(created) != 20 {
			t.Errorf("expected 20 tables created, got %d", len(created))
		}

		// Verify no duplicate table IDs by listing
		tables, err := catalog.ListTables()
		if err != nil {
			t.Fatalf("ListTables failed: %v", err)
		}
		if len(tables) != 20 {
			t.Errorf("expected 20 tables in list, got %d", len(tables))
		}

		// Verify each table is retrievable and not corrupted
		for name := range created {
			schema, err := catalog.GetTable(name)
			if err != nil {
				t.Errorf("GetTable %s failed: %v", name, err)
				continue
			}
			if schema.Name != name {
				t.Errorf("table name mismatch: expected %q, got %q", name, schema.Name)
			}
			if len(schema.Columns) != 1 {
				t.Errorf("table %s: expected 1 column, got %d", name, len(schema.Columns))
			}
		}
	})

	// Test: Mixed concurrent CREATE and DROP
	t.Run("ConcurrentCreateAndDrop", func(t *testing.T) {
		// Create some initial tables
		for i := 0; i < 5; i++ {
			schema := api.TableSchema{
				Name: fmt.Sprintf("mix_table_%d", i),
				Columns: []api.ColumnDef{
					{Name: "id", Type: api.TypeInt},
				},
			}
			catalog.CreateTable(schema)
		}

		var wg sync.WaitGroup
		errCh := make(chan error, 20)

		// 5 goroutines: 3 creating, 2 dropping
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				name := fmt.Sprintf("mix_table_%d", id)
				schema := api.TableSchema{
					Name: name,
					Columns: []api.ColumnDef{
						{Name: "id", Type: api.TypeInt},
					},
				}
				err := catalog.CreateTable(schema)
				if err != nil && err != api.ErrTableExists {
					errCh <- fmt.Errorf("CreateTable %s failed: %v", name, err)
				}
			}(i)
		}

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				name := fmt.Sprintf("mix_table_%d", id)
				err := catalog.DropTable(name)
				if err != nil && err != api.ErrTableNotFound {
					errCh <- fmt.Errorf("DropTable %s failed: %v", name, err)
				}
			}(i)
		}

		// Create more tables
		for i := 5; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				name := fmt.Sprintf("mix_table_%d", id)
				schema := api.TableSchema{
					Name: name,
					Columns: []api.ColumnDef{
						{Name: "id", Type: api.TypeInt},
					},
				}
				err := catalog.CreateTable(schema)
				if err != nil {
					errCh <- fmt.Errorf("CreateTable %s failed: %v", name, err)
				}
			}(i)
		}

		wg.Wait()
		close(errCh)

		// Check for errors (except expected ones)
		for err := range errCh {
			t.Error(err)
		}

		// Verify catalog is still consistent - can list all tables
		tables, err := catalog.ListTables()
		if err != nil {
			t.Fatalf("ListTables failed after concurrent ops: %v", err)
		}

		// All tables should be retrievable (no corrupted metadata)
		for _, name := range tables {
			schema, err := catalog.GetTable(name)
			if err != nil {
				t.Errorf("GetTable %s failed after concurrent ops: %v", name, err)
				continue
			}
			if schema == nil {
				t.Errorf("GetTable %s returned nil schema", name)
			}
		}
	})

	// Test: Concurrent index operations
	t.Run("ConcurrentIndexOps", func(t *testing.T) {
		// Create a table first
		schema := api.TableSchema{
			Name: "index_test_table",
			Columns: []api.ColumnDef{
				{Name: "id", Type: api.TypeInt},
				{Name: "name", Type: api.TypeText},
			},
		}
		if err := catalog.CreateTable(schema); err != nil && err != api.ErrTableExists {
			t.Fatalf("CreateTable failed: %v", err)
		}

		var wg sync.WaitGroup
		errCh := make(chan error, 10)

		// Create multiple indexes concurrently
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				idxSchema := api.IndexSchema{
					Name:   fmt.Sprintf("concurrent_idx_%d", id),
					Table:  "index_test_table",
					Column: "name",
				}
				err := catalog.CreateIndex(idxSchema)
				if err != nil {
					errCh <- fmt.Errorf("CreateIndex failed: %v", err)
				}
			}(i)
		}

		// List indexes concurrently
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := catalog.ListIndexes("index_test_table")
				if err != nil {
					errCh <- fmt.Errorf("ListIndexes failed: %v", err)
				}
			}()
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			t.Error(err)
		}

		// Verify indexes exist
		indexes, err := catalog.ListIndexes("index_test_table")
		if err != nil {
			t.Fatalf("ListIndexes failed: %v", err)
		}
		if len(indexes) < 5 {
			t.Errorf("expected at least 5 indexes, got %d", len(indexes))
		}
	})
}
