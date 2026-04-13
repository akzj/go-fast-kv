package internal

import (
	"strings"
	"unicode"

	"github.com/akzj/go-fast-kv/internal/sql/parser/api"
)

// keywords maps uppercase keyword strings to their token types.
var keywords = map[string]api.TokenType{
	"SELECT":  api.TokSelect,
	"FROM":    api.TokFrom,
	"WHERE":   api.TokWhere,
	"INSERT":  api.TokInsert,
	"INTO":    api.TokInto,
	"VALUES":  api.TokValues,
	"DELETE":  api.TokDelete,
	"UPDATE":  api.TokUpdate,
	"SET":     api.TokSet,
	"CREATE":  api.TokCreate,
	"DROP":    api.TokDrop,
	"TABLE":   api.TokTable,
	"INDEX":   api.TokIndex,
	"ON":      api.TokOn,
	"AND":     api.TokAnd,
	"OR":      api.TokOr,
	"NOT":     api.TokNot,
	"NULL":    api.TokNull,
	"IS":      api.TokIs,
	"ORDER":   api.TokOrder,
	"BY":      api.TokBy,
	"ASC":     api.TokAsc,
	"DESC":    api.TokDesc,
	"LIMIT":   api.TokLimit,
	"INT":     api.TokIntKw,
	"INTEGER": api.TokInteger2,
	"TEXT":    api.TokTextKw,
	"FLOAT":  api.TokFloatKw,
	"BLOB":   api.TokBlobKw,
	"PRIMARY": api.TokPrimary,
	"KEY":     api.TokKey,
	"UNIQUE":  api.TokUnique,
	"IF":      api.TokIf,
	"EXISTS":  api.TokExists,
}

// lexer tokenizes SQL input.
type lexer struct {
	input []byte
	pos   int // current byte position
}

// newLexer creates a new lexer for the given SQL input.
func newLexer(input string) *lexer {
	return &lexer{input: []byte(input), pos: 0}
}

// nextToken returns the next token from the input.
func (l *lexer) nextToken() api.Token {
	l.skipWhitespaceAndComments()

	if l.pos >= len(l.input) {
		return api.Token{Type: api.TokEOF, Literal: "", Pos: l.pos}
	}

	ch := l.input[l.pos]
	startPos := l.pos

	// Single and multi-character operators
	switch ch {
	case '=':
		l.pos++
		return api.Token{Type: api.TokEQ, Literal: "=", Pos: startPos}
	case ',':
		l.pos++
		return api.Token{Type: api.TokComma, Literal: ",", Pos: startPos}
	case '(':
		l.pos++
		return api.Token{Type: api.TokLParen, Literal: "(", Pos: startPos}
	case ')':
		l.pos++
		return api.Token{Type: api.TokRParen, Literal: ")", Pos: startPos}
	case ';':
		l.pos++
		return api.Token{Type: api.TokSemicolon, Literal: ";", Pos: startPos}
	case '+':
		l.pos++
		return api.Token{Type: api.TokPlus, Literal: "+", Pos: startPos}
	case '-':
		l.pos++
		return api.Token{Type: api.TokMinus, Literal: "-", Pos: startPos}
	case '*':
		l.pos++
		return api.Token{Type: api.TokStar, Literal: "*", Pos: startPos}
	case '<':
		l.pos++
		if l.pos < len(l.input) {
			if l.input[l.pos] == '=' {
				l.pos++
				return api.Token{Type: api.TokLE, Literal: "<=", Pos: startPos}
			}
			if l.input[l.pos] == '>' {
				l.pos++
				return api.Token{Type: api.TokNE, Literal: "<>", Pos: startPos}
			}
		}
		return api.Token{Type: api.TokLT, Literal: "<", Pos: startPos}
	case '>':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return api.Token{Type: api.TokGE, Literal: ">=", Pos: startPos}
		}
		return api.Token{Type: api.TokGT, Literal: ">", Pos: startPos}
	case '!':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return api.Token{Type: api.TokNE, Literal: "!=", Pos: startPos}
		}
		return api.Token{Type: api.TokIllegal, Literal: "!", Pos: startPos}
	case '\'':
		return l.readString()
	}

	// Numbers
	if isDigit(ch) {
		return l.readNumber()
	}

	// Identifiers and keywords
	if isIdentStart(ch) {
		return l.readIdentOrKeyword()
	}

	// Unknown character
	l.pos++
	return api.Token{Type: api.TokIllegal, Literal: string(ch), Pos: startPos}
}

// skipWhitespaceAndComments skips whitespace and -- line comments.
func (l *lexer) skipWhitespaceAndComments() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.pos++
			continue
		}
		// -- line comment
		if ch == '-' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			l.pos += 2
			for l.pos < len(l.input) && l.input[l.pos] != '\n' {
				l.pos++
			}
			continue
		}
		break
	}
}

// readString reads a single-quoted string literal.
// Handles '' escape for embedded single quotes.
func (l *lexer) readString() api.Token {
	startPos := l.pos
	l.pos++ // skip opening '
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			l.pos++
			// Check for '' escape
			if l.pos < len(l.input) && l.input[l.pos] == '\'' {
				sb.WriteByte('\'')
				l.pos++
				continue
			}
			// End of string
			return api.Token{Type: api.TokString, Literal: sb.String(), Pos: startPos}
		}
		sb.WriteByte(ch)
		l.pos++
	}
	// Unterminated string
	return api.Token{Type: api.TokIllegal, Literal: sb.String(), Pos: startPos}
}

// readNumber reads an integer or float literal.
func (l *lexer) readNumber() api.Token {
	startPos := l.pos
	isFloat := false
	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}
	// Check for decimal point
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		if l.pos+1 < len(l.input) && isDigit(l.input[l.pos+1]) {
			isFloat = true
			l.pos++ // skip '.'
			for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
				l.pos++
			}
		}
	}
	literal := string(l.input[startPos:l.pos])
	if isFloat {
		return api.Token{Type: api.TokFloat, Literal: literal, Pos: startPos}
	}
	return api.Token{Type: api.TokInteger, Literal: literal, Pos: startPos}
}

// readIdentOrKeyword reads an identifier or keyword.
func (l *lexer) readIdentOrKeyword() api.Token {
	startPos := l.pos
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	literal := string(l.input[startPos:l.pos])
	upper := strings.ToUpper(literal)

	// Check if it's a keyword
	if tokType, ok := keywords[upper]; ok {
		return api.Token{Type: tokType, Literal: upper, Pos: startPos}
	}
	// It's an identifier — normalize to uppercase (review S9)
	return api.Token{Type: api.TokIdent, Literal: upper, Pos: startPos}
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	r := rune(ch)
	return isIdentStart(ch) || isDigit(ch) || unicode.IsDigit(r)
}
