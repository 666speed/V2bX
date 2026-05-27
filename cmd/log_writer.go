package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const logFileRetention = 24 * time.Hour
const logFileRotateInterval = time.Hour
const logFileCheckInterval = time.Minute
const logFileTimeLayout = "20060102150405"

type retentionLogWriter struct {
	path      string
	file      *os.File
	openedAt  time.Time
	lastCheck time.Time
	mu        sync.Mutex
}

func newRetentionLogWriter(path string) (*retentionLogWriter, error) {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	now := time.Now()
	openedAt := now
	if info, err := os.Stat(path); err == nil {
		openedAt = info.ModTime()
		if info.Size() > 0 && now.Sub(openedAt) >= logFileRetention {
			if err := os.Remove(path); err != nil {
				return nil, err
			}
			openedAt = now
		} else if info.Size() > 0 && now.Sub(openedAt) >= logFileRotateInterval {
			if err := rotateExistingLog(path, openedAt); err != nil {
				return nil, err
			}
			openedAt = now
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	writer := &retentionLogWriter{
		path:      path,
		file:      file,
		openedAt:  openedAt,
		lastCheck: now,
	}
	writer.cleanupOldLogs(now)
	return writer, nil
}

func (w *retentionLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	if now.Sub(w.lastCheck) >= logFileCheckInterval {
		w.lastCheck = now
		if now.Sub(w.openedAt) >= logFileRotateInterval {
			if err := w.rotate(now); err != nil {
				return 0, err
			}
		}
		w.cleanupOldLogs(now)
	}

	return w.file.Write(p)
}

func (w *retentionLogWriter) rotate(now time.Time) error {
	if w.file != nil {
		_ = w.file.Close()
	}

	if info, err := os.Stat(w.path); err == nil && info.Size() > 0 {
		rotatedAt := info.ModTime()
		if rotatedAt.IsZero() || rotatedAt.After(now) {
			rotatedAt = now
		}
		if err := rotateExistingLog(w.path, rotatedAt); err != nil {
			return err
		}
	}

	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	w.file = file
	w.openedAt = now
	w.cleanupOldLogs(now)
	return nil
}

func rotateExistingLog(path string, now time.Time) error {
	return os.Rename(path, nextRotatedLogPath(path, now))
}

func nextRotatedLogPath(path string, now time.Time) string {
	stamp := now.Format(logFileTimeLayout)
	candidate := path + "." + stamp
	for i := 1; fileExists(candidate); i++ {
		candidate = fmt.Sprintf("%s.%s.%d", path, stamp, i)
	}
	return candidate
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (w *retentionLogWriter) cleanupOldLogs(now time.Time) {
	dir := filepath.Dir(w.path)
	prefix := filepath.Base(w.path) + "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	cutoff := now.Add(-logFileRetention)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}

		fullPath := filepath.Join(dir, entry.Name())
		logTime, ok := rotatedLogTime(entry.Name(), prefix)
		if !ok {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			logTime = info.ModTime()
		}

		if logTime.Before(cutoff) {
			_ = os.Remove(fullPath)
		}
	}
}

func rotatedLogTime(name string, prefix string) (time.Time, bool) {
	suffix := strings.TrimPrefix(name, prefix)
	if len(suffix) < len(logFileTimeLayout) {
		return time.Time{}, false
	}

	value, err := time.ParseInLocation(logFileTimeLayout, suffix[:len(logFileTimeLayout)], time.Local)
	if err != nil {
		return time.Time{}, false
	}
	return value, true
}
