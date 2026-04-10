// Package checkpoint implements the CheckpointManager.
package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	checkpointapi "github.com/akzj/go-fast-kv/internal/checkpoint/api"
)

// metadataFileName is the checkpoint metadata file name.
const metadataFileName = "metadata.json"

// metadataTempName is the temporary file used during atomic write.
const metadataTempName = "metadata.json.tmp"

// writeMetadata writes the checkpoint metadata atomically.
func writeMetadata(dir string, meta *checkpointapi.Metadata) error {
	// Serialize to JSON
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("checkpoint: marshal metadata: %w", err)
	}

	// Write to temp file
	tmpPath := filepath.Join(dir, metadataTempName)
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("checkpoint: write temp: %w", err)
	}

	// Atomic rename
	metaPath := filepath.Join(dir, metadataFileName)
	if err := os.Rename(tmpPath, metaPath); err != nil {
		return fmt.Errorf("checkpoint: rename: %w", err)
	}

	// Sync directory (ensures the rename is durable)
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("checkpoint: open dir: %w", err)
	}
	if err := dirFile.Sync(); err != nil {
		dirFile.Close()
		return fmt.Errorf("checkpoint: sync dir: %w", err)
	}
	dirFile.Close()

	return nil
}

// readMetadata reads the checkpoint metadata.
func readMetadata(dir string) (*checkpointapi.Metadata, error) {
	metaPath := filepath.Join(dir, metadataFileName)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, checkpointapi.ErrNoCheckpoint
		}
		return nil, fmt.Errorf("checkpoint: read: %w", err)
	}

	var meta checkpointapi.Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("checkpoint: unmarshal: %w", err)
	}

	return &meta, nil
}

// metadataExists checks if a checkpoint metadata file exists.
func metadataExists(dir string) bool {
	metaPath := filepath.Join(dir, metadataFileName)
	_, err := os.Stat(metaPath)
	return err == nil
}
