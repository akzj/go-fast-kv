package internal

import (
	"os"
)

// Helper functions for file operations to make testing easier.

func createFile(path string) (*os.File, error) {
	return os.Create(path)
}

func openFile(path string) (*os.File, error) {
	return os.Open(path)
}

func readAll(f *os.File) ([]byte, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	data := make([]byte, info.Size())
	_, err = f.ReadAt(data, 0)
	return data, err
}

func getFileSize(_ *os.File) int64 {
	return 0
}
