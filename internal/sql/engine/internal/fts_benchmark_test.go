package internal

import (
	"strings"
	"testing"

	kvstore "github.com/akzj/go-fast-kv/internal/kvstore"
	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
)

// ─── Benchmark Helpers ─────────────────────────────────────────────

func openBenchmarkStore(b *testing.B) kvstoreapi.Store {
	b.Helper()
	dir := b.TempDir()
	store, err := kvstore.Open(kvstoreapi.Config{Dir: dir})
	if err != nil {
		b.Fatalf("failed to open store: %v", err)
	}
	b.Cleanup(func() { store.Close() })
	return store
}

func newFTSEngine(b *testing.B, store kvstoreapi.Store) *ftsEngine {
	b.Helper()
	return NewFTSEngine(store).(*ftsEngine)
}

// generateText creates a text string with roughly nWords words.
func generateText(nWords int) string {
	var sb strings.Builder
	words := []string{
		"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
		"database", "sql", "query", "table", "index", "column", "row",
		"search", "engine", "performance", "benchmark", "test", "data",
		"full", "text", "indexing", "tokenize", "stemmer", "porter",
		"document", "match", "result", "term", "operator",
	}
	for i := 0; i < nWords; i++ {
		if i > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(words[i%len(words)])
	}
	return sb.String()
}

// ─── IndexDocument Benchmarks ───────────────────────────────────────

func BenchmarkFTS_IndexDocument_SmallText(b *testing.B) {
	store := openBenchmarkStore(b)
	fts := newFTSEngine(b, store)
	texts := []string{generateText(5)}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := fts.IndexDocument("test_table", uint64(i+1), texts, "porter"); err != nil {
			b.Fatalf("IndexDocument failed: %v", err)
		}
	}
}

func BenchmarkFTS_IndexDocument_MediumText(b *testing.B) {
	store := openBenchmarkStore(b)
	fts := newFTSEngine(b, store)
	texts := []string{generateText(50)}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := fts.IndexDocument("test_table", uint64(i+1), texts, "porter"); err != nil {
			b.Fatalf("IndexDocument failed: %v", err)
		}
	}
}

func BenchmarkFTS_IndexDocument_LargeText(b *testing.B) {
	store := openBenchmarkStore(b)
	fts := newFTSEngine(b, store)
	texts := []string{generateText(200)}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := fts.IndexDocument("test_table", uint64(i+1), texts, "porter"); err != nil {
			b.Fatalf("IndexDocument failed: %v", err)
		}
	}
}

// ─── Tokenize Benchmarks ───────────────────────────────────────────

func BenchmarkFTS_Tokenize_Small(b *testing.B) {
	fts := newFTSEngine(b, openBenchmarkStore(b))
	text := generateText(10)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tokens := fts.tokenize([]string{text}, "porter")
		if len(tokens) == 0 {
			b.Fatal("expected tokens")
		}
	}
}

func BenchmarkFTS_Tokenize_Medium(b *testing.B) {
	fts := newFTSEngine(b, openBenchmarkStore(b))
	text := generateText(50)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tokens := fts.tokenize([]string{text}, "porter")
		if len(tokens) == 0 {
			b.Fatal("expected tokens")
		}
	}
}

func BenchmarkFTS_Tokenize_Large(b *testing.B) {
	fts := newFTSEngine(b, openBenchmarkStore(b))
	text := generateText(200)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tokens := fts.tokenize([]string{text}, "porter")
		if len(tokens) == 0 {
			b.Fatal("expected tokens")
		}
	}
}

// ─── Porter Stemmer Benchmarks ─────────────────────────────────────

func BenchmarkFTS_PorterStem(b *testing.B) {
	words := []string{"running", "played", "jumping", "databases", "queries", "indexing"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, word := range words {
			_ = porterStem(word)
		}
	}
}

// ─── Search Benchmarks ──────────────────────────────────────────────

func setupSearchIndex(b *testing.B, nDocs int) *ftsEngine {
	store := openBenchmarkStore(b)
	fts := newFTSEngine(b, store)
	texts := generateTexts(nDocs, 20, 40)

	for i := 0; i < nDocs; i++ {
		if err := fts.IndexDocument("search_table", uint64(i+1), []string{texts[i]}, "porter"); err != nil {
			b.Fatalf("Setup IndexDocument failed: %v", err)
		}
	}
	return fts
}

func generateTexts(n, minWords, maxWords int) []string {
	texts := make([]string, n)
	for i := 0; i < n; i++ {
		wordCount := minWords + (i % (maxWords - minWords + 1))
		texts[i] = generateText(wordCount)
	}
	return texts
}

func BenchmarkFTS_Search_Term(b *testing.B) {
	fts := setupSearchIndex(b, 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = fts.Search("search_table", "database")
	}
}

func BenchmarkFTS_Search_AND(b *testing.B) {
	fts := setupSearchIndex(b, 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = fts.Search("search_table", "database AND sql")
	}
}

func BenchmarkFTS_Search_OR(b *testing.B) {
	fts := setupSearchIndex(b, 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = fts.Search("search_table", "database OR query")
	}
}

func BenchmarkFTS_Search_NOT(b *testing.B) {
	fts := setupSearchIndex(b, 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = fts.Search("search_table", "database NOT sql")
	}
}

func BenchmarkFTS_Search_Complex(b *testing.B) {
	fts := setupSearchIndex(b, 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = fts.Search("search_table", "database AND sql NOT test")
	}
}

// ─── Dataset Size Comparison ───────────────────────────────────────

func BenchmarkFTS_Search_100Docs(b *testing.B) {
	fts := setupSearchIndex(b, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = fts.Search("search_table", "database")
	}
}

func BenchmarkFTS_Search_1000Docs(b *testing.B) {
	fts := setupSearchIndex(b, 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = fts.Search("search_table", "database")
	}
}

func BenchmarkFTS_Search_5000Docs(b *testing.B) {
	fts := setupSearchIndex(b, 5000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = fts.Search("search_table", "database")
	}
}

// ─── getDocIDsForToken Benchmark ───────────────────────────────────

func BenchmarkFTS_getDocIDsForToken(b *testing.B) {
	fts := setupSearchIndex(b, 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		docIDs, err := fts.getDocIDsForToken("search_table", "database")
		if err != nil {
			b.Fatalf("getDocIDsForToken failed: %v", err)
		}
		if docIDs == nil {
			b.Fatal("expected docIDs")
		}
	}
}

// ─── RemoveDocument Benchmark ─────────────────────────────────────

func BenchmarkFTS_RemoveDocument(b *testing.B) {
	store := openBenchmarkStore(b)
	fts := newFTSEngine(b, store)
	text := generateText(20)

	// Pre-index 100 docs
	for i := 1; i <= 100; i++ {
		fts.IndexDocument("remove_table", uint64(i), []string{text}, "porter")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N && i < 100; i++ {
		_ = fts.RemoveDocument("remove_table", uint64(i+1), []string{text}, "porter")
	}
}