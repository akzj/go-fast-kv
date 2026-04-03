// Package gc provides Garbage Collection for segments.
// Root package re-exports types from api package for backward compatibility.
package gc

import (
	"github.com/akzj/go-fast-kv/pkg/gc/api"
	"github.com/akzj/go-fast-kv/pkg/gc/internal"
)

// Re-export types from api package
type (
	GCController            = api.GCController
	GCPolicy                = api.GCPolicy
	GCStats                 = api.GCStats
	SegmentGCState          = api.SegmentGCState
	ModRateStabilityChecker = api.ModRateStabilityChecker
)

// Re-export internal types
type (
	GCControllerImpl = internal.GCControllerImpl
	SegmentGCMeta    = internal.SegmentGCMeta
)

// Re-export functions
var (
	DefaultGCPolicy = api.DefaultGCPolicy
)
