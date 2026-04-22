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

// queryToken represents a parsed FTS query token.
// Type: "term" or "op" (AND, OR, NOT).
type queryToken struct {
	Type  string // "term", "op"
	Value string // word or operator
}

// tokenizeQuery splits a query string into tokens preserving operators.
func tokenizeQuery(query string) []queryToken {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}

	var tokens []queryToken
	// Use a simple state machine to extract terms and operators.
	// Operators: AND, OR, NOT (case-insensitive).
	// Terms: anything else, stripped of surrounding quotes.

	// First, split by operators while preserving them.
	// Match: word1 AND word2 OR word3 NOT word4
	// Split the query around operators
	inQuote := false
	partStart := 0
	for i := 0; i <= len(query); i++ {
		c := rune(0)
		if i < len(query) {
			c = rune(query[i])
		}

		if c == '"' {
			inQuote = !inQuote
			continue
		}

		if !inQuote && i < len(query) {
			// Check if we're at an operator
			remaining := strings.ToUpper(query[i:])
			if strings.HasPrefix(remaining, "AND ") || strings.HasPrefix(remaining, "AND") {
				// Extract the part before this operator
				if i > partStart {
					part := strings.TrimSpace(query[partStart:i])
					part = strings.Trim(part, "\"")
					if part != "" {
						tokens = append(tokens, queryToken{Type: "term", Value: strings.ToLower(part)})
					}
				}
				// Consume "AND" and any trailing space
				tokens = append(tokens, queryToken{Type: "op", Value: "AND"})
				i += 3 // skip "AND" (i now at space or end)
				for i < len(query) && query[i] == ' ' {
					i++
				}
				// Set partStart to first non-space char, compensate for for-loop's i++
				partStart = i
				i-- // compensate
				continue
			}
			if strings.HasPrefix(remaining, "OR ") || strings.HasPrefix(remaining, "OR") {
				if i > partStart {
					part := strings.TrimSpace(query[partStart:i])
					part = strings.Trim(part, "\"")
					if part != "" {
						tokens = append(tokens, queryToken{Type: "term", Value: strings.ToLower(part)})
					}
				}
				tokens = append(tokens, queryToken{Type: "op", Value: "OR"})
				i += 2 // skip "OR" (i now at space or end)
				for i < len(query) && query[i] == ' ' {
					i++
				}
				// Set partStart to first non-space char, compensate for for-loop's i++
				partStart = i
				i-- // compensate
				continue
			}
			if strings.HasPrefix(remaining, "NOT ") || strings.HasPrefix(remaining, "NOT") {
				if i > partStart {
					part := strings.TrimSpace(query[partStart:i])
					part = strings.Trim(part, "\"")
					if part != "" {
						tokens = append(tokens, queryToken{Type: "term", Value: strings.ToLower(part)})
					}
				}
				tokens = append(tokens, queryToken{Type: "op", Value: "NOT"})
				i += 3 // skip "NOT" (i now at space or end)
				for i < len(query) && query[i] == ' ' {
					i++
				}
				// Set partStart to first non-space char, compensate for for-loop's i++
				partStart = i
				i-- // compensate
				continue
			}
		}
	}

	// Handle remaining part after last operator
	if partStart < len(query) {
		part := strings.TrimSpace(query[partStart:])
		part = strings.Trim(part, "\"")
		if part != "" {
			tokens = append(tokens, queryToken{Type: "term", Value: strings.ToLower(part)})
		}
	}

	return tokens
}

// parseFTSQuery parses a simple FTS query string.
// Supports: term, "term1 AND term2", "term1 OR term2", "term1 NOT term2"
// Simple terms are treated as AND (all must match).
func parseFTSQuery(query string) []string {
	tokens := tokenizeQuery(query)
	if len(tokens) == 0 {
		return nil
	}

	// For backward compatibility: if no operators, just return terms.
	hasOp := false
	for _, t := range tokens {
		if t.Type == "op" {
			hasOp = true
			break
		}
	}

	if !hasOp {
		// No operators - return all terms as simple list (AND semantics)
		var terms []string
		for _, t := range tokens {
			if t.Type == "term" && t.Value != "" {
				terms = append(terms, t.Value)
			}
		}
		return terms
	}

	// Legacy: strip operators and return terms
	var terms []string
	for _, t := range tokens {
		if t.Type == "term" {
			terms = append(terms, t.Value)
		}
	}
	return terms
}

// Search performs an FTS search and returns matching docIDs.
// Supports AND (intersection), OR (union), NOT (difference).
func (f *ftsEngine) Search(tableName string, query string) ([]uint64, error) {
	tokens := tokenizeQuery(query)

	if len(tokens) == 0 {
		return nil, nil
	}

	// Check if we have any operators
	hasOp := false
	for _, t := range tokens {
		if t.Type == "op" {
			hasOp = true
			break
		}
	}

	if !hasOp {
		// No operators - simple single term search
		term := ""
		if tokens[0].Type == "term" {
			term = tokens[0].Value
		}
		if term == "" {
			return nil, nil
		}
		docIDs, err := f.getDocIDsForToken(tableName, term)
		if err != nil {
			return nil, err
		}
		return docIDsToList(docIDs), nil
	}

	// Execute query with operators
	return f.executeQuery(tableName, tokens)
}

// executeQuery evaluates a tokenized query with AND/OR/NOT operators.
// Operator precedence: NOT > AND > OR (left-to-right for same precedence).
func (f *ftsEngine) executeQuery(tableName string, tokens []queryToken) ([]uint64, error) {
	if len(tokens) == 0 {
		return nil, nil
	}

	// First pass: handle NOT (unary operator, highest precedence)
	// "word1 NOT word2" means docs with word1 that don't have word2
	// We process NOT by computing the set difference

	// For simplicity, let's implement left-to-right evaluation with
	// NOT having higher precedence than AND/OR.
	// Algorithm: 
	// 1. Group into groups separated by OR
	// 2. For each group, compute AND/NOT
	// 3. OR the groups together

	// Alternative: simpler approach - compute all term sets first,
	// then apply operators in order.

	// Get all term sets
	type termSet struct {
		term  string
		negate bool
	}
	var termSets []termSet
	var ops []string // operators between terms

	currentNegate := false
	for i, token := range tokens {
		if token.Type == "op" {
			ops = append(ops, token.Value)
			if token.Value == "NOT" {
				// NOT is a unary operator - next term should be negated
				currentNegate = true
			}
		} else {
			termSets = append(termSets, termSet{
				term:   token.Value,
				negate: currentNegate,
			})
			currentNegate = false
			_ = i // silence unused warning
		}
	}

	// Evaluate left to right with AND/OR precedence:
	// AND has higher precedence than OR
	// "a OR b AND c" = "(a OR b) AND c" = left-to-right

	// Group by OR first, then AND within groups
	var orGroups [][]termSet
	var currentGroup []termSet

	for i, ts := range termSets {
		currentGroup = append(currentGroup, ts)
		if i < len(ops) && ops[i] == "OR" {
			orGroups = append(orGroups, currentGroup)
			currentGroup = nil
		}
	}
	if len(currentGroup) > 0 {
		orGroups = append(orGroups, currentGroup)
	}

	// Evaluate each OR group (AND within)
	var groupResults []map[uint64]struct{}
	for _, group := range orGroups {
		if len(group) == 0 {
			continue
		}

		// Separate negated and non-negated terms
		var posTerms []string      // terms that must match (AND)
		var negTerms []string      // terms that must NOT match (NOT)

		for _, ts := range group {
			if ts.negate {
				negTerms = append(negTerms, ts.term)
			} else {
				posTerms = append(posTerms, ts.term)
			}
		}

		// Get docIDs for positive terms (intersection = AND)
		var result map[uint64]struct{}
		for i, term := range posTerms {
			docIDs, err := f.getDocIDsForToken(tableName, term)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				result = docIDs
			} else {
				result = intersectDocIDs([]map[uint64]struct{}{result, docIDs})
			}
		}

		// If no positive terms, start with all docs from negated term's table
		// (This is a simplified approach - for "NOT word", start with word's docs as "all")
		if len(posTerms) == 0 && len(negTerms) > 0 {
			// For "NOT X", we need all docs minus X's docs
			// But we don't know "all docs" - use negated term's docs as the exclusion set
			// This works for "sql NOT database" where sql's docs = {1, 3}, database's docs = {2, 3}
			// Result = sql docs minus database docs = {1}
			for _, term := range negTerms {
				docIDs, err := f.getDocIDsForToken(tableName, term)
				if err != nil {
					return nil, err
				}
				if result == nil {
					result = docIDs
				} else {
					// For multiple NOT terms, union them first
					result = unionDocIDs([]map[uint64]struct{}{result, docIDs})
				}
			}
		}

		// Get docIDs for negated terms (union = any of these excludes)
		if len(negTerms) > 0 {
			var negatedIDs map[uint64]struct{}
			for i, term := range negTerms {
				docIDs, err := f.getDocIDsForToken(tableName, term)
				if err != nil {
					return nil, err
				}
				if i == 0 {
					negatedIDs = docIDs
				} else {
					negatedIDs = unionDocIDs([]map[uint64]struct{}{negatedIDs, docIDs})
				}
			}
			// Subtract all negated docIDs from result
			if result != nil && len(negatedIDs) > 0 {
				for docID := range negatedIDs {
					delete(result, docID)
				}
			}
		}

		if result != nil {
			groupResults = append(groupResults, result)
		}
	}

	if len(groupResults) == 0 {
		return nil, nil
	}

	// OR = union of all groups
	result := unionDocIDs(groupResults)
	return docIDsToList(result), nil
}

// docIDsToList converts a map to sorted slice.
func docIDsToList(docIDs map[uint64]struct{}) []uint64 {
	list := make([]uint64, 0, len(docIDs))
	for docID := range docIDs {
		list = append(list, docID)
	}
	sort.Slice(list, func(i, j int) bool { return list[i] < list[j] })
	return list
}

// unionDocIDs returns the union of multiple docID sets.
func unionDocIDs(sets []map[uint64]struct{}) map[uint64]struct{} {
	if len(sets) == 0 {
		return make(map[uint64]struct{})
	}
	if len(sets) == 1 {
		return sets[0]
	}

	result := make(map[uint64]struct{})
	for _, set := range sets {
		for docID := range set {
			result[docID] = struct{}{}
		}
	}
	return result
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
