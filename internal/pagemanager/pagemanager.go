// Package pagemanager provides PageID → VAddr mapping and page allocation.
package pagemanager

import (
	storage "github.com/akzj/go-fast-kv/internal/storage"
	api "github.com/akzj/go-fast-kv/internal/pagemanager/api"
	internal "github.com/akzj/go-fast-kv/internal/pagemanager/internal"
)

// OpenPageManager creates a new PageManager with the given segment manager.
func OpenPageManager(segmentManager storage.SegmentManager) (api.PageManager, error) {
	return internal.NewPageManager(segmentManager, internal.DefaultPageManagerConfig())
}

// OpenPageManagerWithConfig creates a new PageManager with custom configuration.
func OpenPageManagerWithConfig(segmentManager storage.SegmentManager, config internal.PageManagerConfig) (api.PageManager, error) {
	return internal.NewPageManager(segmentManager, config)
}
