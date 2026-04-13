// Package parser provides SQL parsing.
package parser

import (
	"fmt"
)

// TokenType represents a token type.
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenIdent
	TokenInt
	TokenFloat
	TokenString
	TokenKeyword
	TokenOperator
	TokenPunct
)

// Token represents a lexical token.
type Token struct {
	Type  TokenType
	Value string
	Pos   int
}

func (t Token) String() string {
	switch t.Type {
	case TokenEOF:
		return "EOF"
	case TokenIdent:
		return fmt.Sprintf("IDENT(%s)", t.Value)
	case TokenInt:
		return fmt.Sprintf("INT(%s)", t.Value)
	case TokenFloat:
		return fmt.Sprintf("FLOAT(%s)", t.Value)
	case TokenString:
		return fmt.Sprintf("STRING(%s)", t.Value)
	case TokenKeyword:
		return fmt.Sprintf("KW(%s)", t.Value)
	case TokenOperator:
		return fmt.Sprintf("OP(%s)", t.Value)
	case TokenPunct:
		return fmt.Sprintf("PUNCT(%s)", t.Value)
	default:
		return fmt.Sprintf("?(%s)", t.Value)
	}
}

// Keywords maps keywords to their token types.
var Keywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true,
	"INSERT": true, "INTO": true, "VALUES": true,
	"UPDATE": true, "SET": true,
	"DELETE": true,
	"CREATE": true, "TABLE": true, "INDEX": true, "DROP": true,
	"AND": true, "OR": true,
	"ORDER": true, "BY": true, "ASC": true, "DESC": true,
	"LIMIT": true,
	"NULL": true,
	"UNIQUE": true, "PRIMARY": true, "KEY": true, "ON": true,
}

// TypeKeywords are type names that should be parsed as identifiers in column definitions.
var TypeKeywords = map[string]bool{
	"INT": true, "INTEGER": true, "BIGINT": true,
	"FLOAT": true, "REAL": true, "DOUBLE": true, "DECIMAL": true,
	"TEXT": true, "VARCHAR": true, "CHAR": true, "STRING": true,
	"BLOB": true, "BYTES": true,
	"BOOL": true, "BOOLEAN": true,
	"DATE": true, "TIME": true, "TIMESTAMP": true,
}

// Lexer tokenizes SQL input.
type Lexer struct {
	input  string
	pos    int
	start  int
	tokens []Token
}

// NewLexer creates a new Lexer.
func NewLexer(input string) *Lexer {
	return &Lexer{input: input, pos: 0, start: 0}
}

// Next returns the next token.
func (l *Lexer) Next() Token {
	l.skipWhitespace()
	l.start = l.pos
	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF, Pos: l.pos}
	}

	ch := l.input[l.pos]

	// Numbers
	if isDigit(ch) {
		return l.scanNumber()
	}

	// Identifiers and keywords
	if isAlpha(ch) || ch == '_' {
		return l.scanIdent()
	}

	// Strings
	if ch == '\'' || ch == '"' {
		return l.scanString()
	}

	// Operators (2-char first)
	if l.pos+1 < len(l.input) {
		two := string(ch) + string(l.input[l.pos+1])
		switch two {
		case "<=", ">=", "!=", "==", "->":
			l.pos += 2
			return Token{Type: TokenOperator, Value: two, Pos: l.start}
		}
	}

	// Single char operators
	l.pos++
	switch ch {
	case '=', '<', '>', '+', '-', '*', '/', '%':
		return Token{Type: TokenOperator, Value: string(ch), Pos: l.start}
	case '(', ')', ',', ';':
		return Token{Type: TokenPunct, Value: string(ch), Pos: l.start}
	}

	return Token{Type: TokenPunct, Value: string(ch), Pos: l.start}
}

func (l *Lexer) scanNumber() Token {
	start := l.pos
	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}
	// Float?
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		l.pos++
		for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
			l.pos++
		}
		return Token{Type: TokenFloat, Value: l.input[start:l.pos], Pos: start}
	}
	return Token{Type: TokenInt, Value: l.input[start:l.pos], Pos: start}
}

func (l *Lexer) scanIdent() Token {
	start := l.pos
	for l.pos < len(l.input) && (isAlphaNum(l.input[l.pos]) || l.input[l.pos] == '_') {
		l.pos++
	}
	value := l.input[start:l.pos]
	upper := toUpper(value)
	if Keywords[upper] {
		return Token{Type: TokenKeyword, Value: upper, Pos: start}
	}
	// Type keywords are parsed as identifiers
	return Token{Type: TokenIdent, Value: upper, Pos: start}
}

func (l *Lexer) scanString() Token {
	start := l.pos
	quote := l.input[l.pos]
	l.pos++
	for l.pos < len(l.input) && l.input[l.pos] != quote {
		if l.input[l.pos] == '\\' && l.pos+1 < len(l.input) {
			l.pos++
		}
		l.pos++
	}
	if l.pos < len(l.input) {
		l.pos++ // consume closing quote
	}
	return Token{Type: TokenString, Value: l.input[start+1 : l.pos-1], Pos: start}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && isSpace(l.input[l.pos]) {
		l.pos++
	}
}

func isDigit(c byte) bool    { return c >= '0' && c <= '9' }
func isAlpha(c byte) bool    { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isAlphaNum(c byte) bool { return isAlpha(c) || isDigit(c) }
func isSpace(c byte) bool    { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func toUpper(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		result[i] = c
	}
	return string(result)
}
