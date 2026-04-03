// Package vaddr provides foundation types for the append-only storage system.
//
// Usage:
//
//	import vaddr "github.com/akzj/go-fast-kv/internal/vaddr"
//
// This package re-exports all public types from the api package.
package vaddr

import vaddrapi "github.com/akzj/go-fast-kv/internal/vaddr/api"

// Re-export all types from api package.
type (
    VAddr       = vaddrapi.VAddr
    SegmentID   = vaddrapi.SegmentID
    PageID      = vaddrapi.PageID
    SegmentState = vaddrapi.SegmentState
    EpochID     = vaddrapi.EpochID
)

// Re-export constants.
const (
    PageSize            = vaddrapi.PageSize
    ExternalThreshold   = vaddrapi.ExternalThreshold
    MaxSegmentSize       = vaddrapi.MaxSegmentSize
    SegmentHeaderSize    = vaddrapi.SegmentHeaderSize
    SegmentTrailerSize   = vaddrapi.SegmentTrailerSize
    EpochGracePeriod     = vaddrapi.EpochGracePeriod

    // SegmentState constants
    SegmentStateActive   = vaddrapi.SegmentStateActive
    SegmentStateSealed   = vaddrapi.SegmentStateSealed
    SegmentStateArchived = vaddrapi.SegmentStateArchived

    // Invalid IDs
    SegmentIDInvalid = vaddrapi.SegmentIDInvalid
    SegmentIDMin     = vaddrapi.SegmentIDMin
    PageIDInvalid    = vaddrapi.PageIDInvalid
)

// Re-export functions.
var (
    VAddrFromBytes = vaddrapi.VAddrFromBytes
)
