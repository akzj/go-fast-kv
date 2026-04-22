package internal

import (
	"testing"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	"github.com/akzj/go-fast-kv/internal/sql/catalog"
	"github.com/akzj/go-fast-kv/internal/sql/encoding"
	"github.com/akzj/go-fast-kv/internal/sql/engine"
	"github.com/akzj/go-fast-kv/internal/sql/parser"
	"github.com/akzj/go-fast-kv/internal/sql/planner"
	plannerapi "github.com/akzj/go-fast-kv/internal/sql/planner/api"
)

// TestJoinPlanType verifies what plan type is created for different queries.
func TestJoinPlanType(t *testing.T) {
	store, err := kvstore.Open(kvstoreapi.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	cat := catalog.New(store)
	enc := encoding.NewKeyEncoder()
	codec := encoding.NewRowCodec()
	tbl := engine.NewTableEngine(store, enc, codec)
	idx := engine.NewIndexEngine(store, enc)
	p := parser.New()
	pl := planner.New(cat, p)
	ex := New(store, cat, tbl, idx, nil, pl, p)

	// Create tables
	stmt, _ := p.Parse("CREATE TABLE t1 (id INT, a INT)")
	plan, _ := pl.Plan(stmt)
	ex.Execute(plan)
	
	stmt, _ = p.Parse("CREATE TABLE t2 (id INT, x INT)")
	plan, _ = pl.Plan(stmt)
	ex.Execute(plan)

	tests := []struct {
		query    string
		wantType string
	}{
		// Pure equi-join should use HashJoinPlan
		{"SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id", "HashJoinPlan"},
		// Equi-join with extra AND should NOT use HashJoinPlan
		{"SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id AND t1.a > 5", "SelectPlan"},
	}

	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			stmt, err := p.Parse(tc.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			plan, err := pl.Plan(stmt)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}

			selPlan, ok := plan.(*plannerapi.SelectPlan)
			if !ok {
				t.Fatalf("plan type = %T, want *SelectPlan", plan)
			}

			switch selPlan.Join.(type) {
			case *plannerapi.HashJoinPlan:
				t.Logf("Query: %s -> HashJoinPlan", tc.query)
			case *plannerapi.JoinPlan:
				t.Logf("Query: %s -> JoinPlan", tc.query)
			default:
				t.Logf("Query: %s -> %T", tc.query, selPlan.Join)
			}
		})
	}
}
