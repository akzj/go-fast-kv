package internal

import "errors"

// Storage errors.
var (
	ErrSegmentClosed     = errors.New("storage: segment is closed")
	ErrSegmentNotActive  = errors.New("storage: segment is not active")
	ErrSegmentNotSealed  = errors.New("storage: segment is not sealed")
	ErrSegmentFull       = errors.New("storage: segment is full")
	ErrMaxSegments       = errors.New("storage: maximum segment count reached")
	ErrStorageClosed     = errors.New("storage: storage is closed")
	ErrInvalidOffset     = errors.New("storage: invalid offset")
	ErrInvalidSegmentID  = errors.New("storage: invalid segment ID")
	ErrInvalidAlignment  = errors.New("storage: data must be aligned to page size")
	ErrSegmentNotFound   = errors.New("storage: segment not found")
	ErrInvalidFileFormat = errors.New("storage: invalid file format")
)
