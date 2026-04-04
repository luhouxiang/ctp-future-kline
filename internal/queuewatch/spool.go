package queuewatch

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type JSONSpool[T any] struct {
	dir     string
	counter atomic.Uint64

	mu    sync.Mutex
	files []string
}

func NewJSONSpool[T any](rootDir string, queueName string) (*JSONSpool[T], error) {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return nil, nil
	}
	dir := filepath.Join(rootDir, sanitizeQueueName(queueName))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	return &JSONSpool[T]{dir: dir, files: files}, nil
}

func (s *JSONSpool[T]) Enqueue(v T) (int64, error) {
	if s == nil {
		return 0, fmt.Errorf("spool not configured")
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	suffix := s.counter.Add(1)
	name := fmt.Sprintf("%020d-%06d.json", time.Now().UnixNano(), suffix)
	tmp := filepath.Join(s.dir, name+".tmp")
	final := filepath.Join(s.dir, name)
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	s.mu.Lock()
	s.files = append(s.files, name)
	sort.Strings(s.files)
	s.mu.Unlock()
	return int64(len(payload)), nil
}

func (s *JSONSpool[T]) Dequeue() (T, bool, int64, error) {
	var zero T
	if s == nil {
		return zero, false, 0, nil
	}
	s.mu.Lock()
	if len(s.files) == 0 {
		s.mu.Unlock()
		return zero, false, 0, nil
	}
	name := s.files[0]
	s.mu.Unlock()

	path := filepath.Join(s.dir, name)
	payload, err := os.ReadFile(path)
	if err != nil {
		return zero, false, 0, err
	}
	var out T
	if err := json.Unmarshal(payload, &out); err != nil {
		return zero, false, 0, err
	}
	if err := os.Remove(path); err != nil {
		return zero, false, 0, err
	}
	s.mu.Lock()
	if len(s.files) > 0 && s.files[0] == name {
		s.files = s.files[1:]
	} else {
		for i := range s.files {
			if s.files[i] == name {
				s.files = append(s.files[:i], s.files[i+1:]...)
				break
			}
		}
	}
	s.mu.Unlock()
	return out, true, int64(len(payload)), nil
}

func (s *JSONSpool[T]) Pending() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.files)
}

func sanitizeQueueName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "queue"
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' {
			b.WriteRune(ch)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}
