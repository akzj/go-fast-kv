package catalog

import "errors"

var (
	ErrTableNotFound = errors.New("catalog: table not found")
	ErrTableExists   = errors.New("catalog: table already exists")
	ErrColumnNotFound = errors.New("catalog: column not found")
	ErrIndexNotFound = errors.New("catalog: index not found")
	ErrIndexExists   = errors.New("catalog: index already exists")
)
