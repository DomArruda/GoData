package dataframe

import (
	"fmt"
	"os"
)

// writeTempFile stages in-memory bytes to a temporary file so DuckDB's
// file-based readers can consume them. It returns the path and a cleanup
// function the caller must defer. The pattern should contain a single "*"
// which os.CreateTemp replaces with a random string (e.g. "df-*.csv").
func writeTempFile(pattern string, data []byte) (path string, cleanup func(), err error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", func() {}, fmt.Errorf("creating temp file: %w", err)
	}
	name := f.Name()
	cleanup = func() { _ = os.Remove(name) }

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		cleanup()
		return "", func() {}, fmt.Errorf("writing temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("closing temp file: %w", err)
	}
	return name, cleanup, nil
}
