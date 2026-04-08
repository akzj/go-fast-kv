// Package segment provides the Segment Manager — the lowest storage layer.
//
// Design reference: docs/DESIGN.md §3.1
package segment

import (
	segmentapi "github.com/akzj/go-fast-kv/internal/segment/api"
	"github.com/akzj/go-fast-kv/internal/segment/internal"
)

// New creates a new SegmentManager.
func New(cfg segmentapi.Config) (segmentapi.SegmentManager, error) {
	return internal.New(cfg)
}
