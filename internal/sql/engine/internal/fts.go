package internal

import (
	"encoding/binary"
	"sort"
	"strings"
	"unicode"

	kvstoreapi "github.com/akzj/go-fast-kv/internal/kvstore/api"
	engineapi "github.com/akzj/go-fast-kv/internal/sql/engine/api"
)

// Compile-time interface check.
var _ engineapi.FTSEngine = (*ftsEngine)(nil)

// ftsEngine implements engineapi.FTSEngine using an inverted index.
type ftsEngine struct {
	store kvstoreapi.Store
}

// NewFTSEngine creates a new FTSEngine.
func NewFTSEngine(store kvstoreapi.Store) engineapi.FTSEngine {
	return &ftsEngine{store: store}
}

// FTS key helpers.
const (
	ftsIndexKeyPrefix = "_sql:fti:" // prefix for FTS inverted index
)

// ftsIndexKey returns the index key for (tableName, token, docID).
func ftsIndexKey(tableName string, token string, docID uint64) []byte {
	// Key format: _sql:fti:{tableName}:{token}:{docID}
	encoded := encodeToken(token)
	tokenLen := len(encoded)
	docIDLen := 8 // uint64
	nameLen := len(tableName)
	keyLen := len(ftsIndexKeyPrefix) + nameLen + 1 + tokenLen + 1 + docIDLen

	buf := make([]byte, keyLen)
	offset := 0

	// _sql:fti:
	copy(buf[offset:offset+len(ftsIndexKeyPrefix)], ftsIndexKeyPrefix)
	offset += len(ftsIndexKeyPrefix)

	// tableName
	copy(buf[offset:offset+nameLen], tableName)
	offset += nameLen

	buf[offset] = ':'
	offset++

	// token
	copy(buf[offset:offset+tokenLen], encoded)
	offset += tokenLen

	buf[offset] = ':'
	offset++

	// docID
	binary.BigEndian.PutUint64(buf[offset:offset+8], docID)
	return buf
}

// ftsIndexPrefixForTable returns the index prefix for all tokens of a table.
func ftsIndexPrefixForTable(tableName string) []byte {
	// Key format: _sql:fti:{tableName}:
	buf := make([]byte, 0, len(ftsIndexKeyPrefix)+len(tableName)+1)
	buf = append(buf, ftsIndexKeyPrefix...)
	buf = append(buf, tableName...)
	buf = append(buf, ':')
	return buf
}

// ftsIndexPrefixEnd returns the end key for scanning all tokens for a table.
func ftsIndexPrefixEnd(tableName string) []byte {
	prefix := ftsIndexPrefixForTable(tableName)
	end := make([]byte, len(prefix))
	copy(end, prefix)
	end[len(end)-1] = 0xFF
	return end
}

// encodeToken encodes a token for use as a key (lowercase).
func encodeToken(token string) []byte {
	return []byte(strings.ToLower(token))
}

// IndexDocument adds a document to the FTS inverted index.
func (f *ftsEngine) IndexDocument(tableName string, docID uint64, texts []string, tokenizer string) error {
	// Collect all tokens from all texts
	tokens := f.tokenize(texts, tokenizer)

	batch := f.store.NewWriteBatch()
	for _, token := range tokens {
		key := ftsIndexKey(tableName, token, docID)
		if err := batch.Put(key, []byte{}); err != nil {
			batch.Discard()
			return err
		}
	}
	return batch.Commit()
}

// RemoveDocument removes a document from the FTS inverted index.
func (f *ftsEngine) RemoveDocument(tableName string, docID uint64, texts []string, tokenizer string) error {
	tokens := f.tokenize(texts, tokenizer)

	batch := f.store.NewWriteBatch()
	for _, token := range tokens {
		key := ftsIndexKey(tableName, token, docID)
		if err := batch.Delete(key); err != nil {
			batch.Discard()
			return err
		}
	}
	return batch.Commit()
}

// Search performs an FTS search and returns matching docIDs.
func (f *ftsEngine) Search(tableName string, query string) ([]uint64, error) {
	// Parse the query into terms and operators
	terms := parseFTSQuery(query)

	if len(terms) == 0 {
		return nil, nil
	}

	// Get matching docIDs for each term
	var termDocIDs []map[uint64]struct{}
	for _, term := range terms {
		docIDs, err := f.getDocIDsForToken(tableName, term)
		if err != nil {
			return nil, err
		}
		termDocIDs = append(termDocIDs, docIDs)
	}

	if len(termDocIDs) == 0 {
		return nil, nil
	}

	// For AND: intersect all sets
	result := intersectDocIDs(termDocIDs)

	// Convert to sorted slice
	docList := make([]uint64, 0, len(result))
	for docID := range result {
		docList = append(docList, docID)
	}
	sort.Slice(docList, func(i, j int) bool { return docList[i] < docList[j] })

	return docList, nil
}

// getDocIDsForToken returns all docIDs that contain the given token.
func (f *ftsEngine) getDocIDsForToken(tableName string, token string) (map[uint64]struct{}, error) {
	prefix := ftsIndexPrefixForTable(tableName)
	encoded := encodeToken(token)

	startKey := make([]byte, 0, len(prefix)+len(encoded)+1)
	startKey = append(startKey, prefix...)
	startKey = append(startKey, encoded...)
	startKey = append(startKey, ':')

	endKey := make([]byte, 0, len(prefix)+len(encoded)+2)
	endKey = append(endKey, prefix...)
	endKey = append(endKey, encoded...)
	endKey = append(endKey, ':')
	endKey = append(endKey, 0xFF)
	endKey = append(endKey, 0xFF)

	iter := f.store.Scan(startKey, endKey)
	defer iter.Close()

	docIDs := make(map[uint64]struct{})
	for iter.Next() {
		key := iter.Key()
		// Key format: _sql:fti:{tableName}:{token}:{docID}
		if len(key) < 8 {
			continue
		}
		docID := binary.BigEndian.Uint64(key[len(key)-8:])
		docIDs[docID] = struct{}{}
	}
	return docIDs, iter.Err()
}

// DropFTSData deletes all FTS data for a table.
func (f *ftsEngine) DropFTSData(tableName string) error {
	prefix := ftsIndexPrefixForTable(tableName)
	end := ftsIndexPrefixEnd(tableName)
	_, err := f.store.DeleteRange(prefix, end)
	return err
}

// tokenize splits text into tokens.
func (f *ftsEngine) tokenize(texts []string, tokenizer string) []string {
	var tokens []string
	tokenMap := make(map[string]struct{}) // deduplicate

	for _, text := range texts {
		if text == "" {
			continue
		}
		words := simpleTokenize(text)
		for _, word := range words {
			if word == "" {
				continue
			}
			// Apply stemmer if requested
			if tokenizer == "porter" {
				word = porterStem(word)
			}
			if word != "" {
				tokenMap[word] = struct{}{}
			}
		}
	}

	for token := range tokenMap {
		tokens = append(tokens, token)
	}
	return tokens
}

// simpleTokenize splits text on whitespace and strips punctuation.
func simpleTokenize(text string) []string {
	var words []string
	var word strings.Builder

	for _, r := range text {
		if unicode.IsSpace(r) {
			if word.Len() > 0 {
				words = append(words, word.String())
				word.Reset()
			}
			continue
		}
		// Keep alphanumeric characters
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			word.WriteRune(unicode.ToLower(r))
		}
		// Skip punctuation
	}

	if word.Len() > 0 {
		words = append(words, word.String())
	}
	return words
}

// parseFTSQuery parses a simple FTS query string.
// Supports: term, "term1 AND term2", "term1 OR term2"
// Simple terms are treated as AND (all must match).
func parseFTSQuery(query string) []string {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	// Check for explicit operators
	upperQuery := strings.ToUpper(query)
	if strings.Contains(upperQuery, " AND ") {
		parts := strings.Split(strings.ToLower(query), " and ")
		var result []string
		for _, part := range parts {
			part = strings.TrimSpace(part)
			// Remove NOT prefix if present
			if strings.HasPrefix(part, "not ") {
				part = strings.TrimPrefix(part, "not ")
			}
			// Remove quotes
			part = strings.Trim(part, "\"")
			if part != "" {
				result = append(result, part)
			}
		}
		return result
	}

	if strings.Contains(upperQuery, " OR ") {
		parts := strings.Split(strings.ToLower(query), " or ")
		var result []string
		for _, part := range parts {
			part = strings.TrimSpace(part)
			// Remove NOT prefix if present
			if strings.HasPrefix(part, "not ") {
				part = strings.TrimPrefix(part, "not ")
			}
			// Remove quotes
			part = strings.Trim(part, "\"")
			if part != "" {
				result = append(result, part)
			}
		}
		return result
	}

	// Simple single term - remove quotes
	term := strings.Trim(query, "\"")
	return []string{strings.ToLower(term)}
}

// intersectDocIDs returns the intersection of multiple docID sets.
func intersectDocIDs(sets []map[uint64]struct{}) map[uint64]struct{} {
	if len(sets) == 0 {
		return make(map[uint64]struct{})
	}
	if len(sets) == 1 {
		return sets[0]
	}

	result := make(map[uint64]struct{})
	// Start with the first set
	for docID := range sets[0] {
		result[docID] = struct{}{}
	}
	// Intersect with remaining sets
	for i := 1; i < len(sets); i++ {
		for docID := range result {
			if _, ok := sets[i][docID]; !ok {
				delete(result, docID)
			}
		}
	}
	return result
}

// porterStem is a simple porter stemmer implementation.
func porterStem(word string) string {
	if len(word) <= 3 {
		return word
	}

	// Common suffixes (simplified)
	suffixes := []struct {
		suffix string
		stem   string
	}{
		{"ational", "ate"},
		{"tional", "tion"},
		{"enci", "ence"},
		{"anci", "ance"},
		{"izer", "ize"},
		{"ation", "ate"},
		{"alism", "al"},
		{"iveness", "ive"},
		{"fulness", "ful"},
		{"ousness", "ous"},
		{"aliti", "al"},
		{"iviti", "ive"},
		{"biliti", "ble"},
		{"alli", "al"},
		{"entli", "ent"},
		{"eli", "e"},
		{"ousli", "ous"},
		{"ement", ""},
		{"ment", ""},
		{"ent", ""},
		{"ness", ""},
		{"ful", ""},
		{"less", ""},
		{"able", ""},
		{"ible", ""},
		{"al", ""},
		{"ive", ""},
		{"ous", ""},
		{"ant", ""},
		{"ence", ""},
		{"ance", ""},
		{"er", ""},
		{"ic", ""},
		{"ment", ""},
		{"ing", ""},
		{"ion", ""},
		{"ed", ""},
		{"es", ""},
		{"ly", ""},
	}

	for _, s := range suffixes {
		if len(word) > len(s.suffix)+2 && strings.HasSuffix(word, s.suffix) {
			stem := word[:len(word)-len(s.suffix)] + s.stem
			if len(stem) >= 2 {
				return stem
			}
		}
	}

	return word
}
