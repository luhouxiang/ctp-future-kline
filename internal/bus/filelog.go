package bus

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type FileLog struct {
	dir           string
	flushInterval time.Duration

	mu            sync.Mutex
	currentDay    string
	currentPath   string
	file          *os.File
	writer        *bufio.Writer
	lastFlushTime time.Time
}

func NewFileLog(dir string, flushInterval time.Duration) *FileLog {
	return &FileLog{
		dir:           dir,
		flushInterval: flushInterval,
	}
}

func (l *FileLog) Append(ev BusEvent) (FileCursor, error) {
	if strings.TrimSpace(ev.EventID) == "" {
		ev.EventID = NewEventID()
	}
	if ev.ProducedAt.IsZero() {
		ev.ProducedAt = time.Now()
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.ensureWriter(ev.ProducedAt); err != nil {
		return FileCursor{}, err
	}
	offset, err := l.file.Seek(0, 1)
	if err != nil {
		return FileCursor{}, fmt.Errorf("seek file offset failed: %w", err)
	}
	line, err := json.Marshal(ev)
	if err != nil {
		return FileCursor{}, fmt.Errorf("marshal event failed: %w", err)
	}
	if _, err := l.writer.Write(line); err != nil {
		return FileCursor{}, fmt.Errorf("write event failed: %w", err)
	}
	if err := l.writer.WriteByte('\n'); err != nil {
		return FileCursor{}, fmt.Errorf("write newline failed: %w", err)
	}
	if l.flushInterval <= 0 || time.Since(l.lastFlushTime) >= l.flushInterval {
		if err := l.writer.Flush(); err != nil {
			return FileCursor{}, fmt.Errorf("flush event writer failed: %w", err)
		}
		l.lastFlushTime = time.Now()
	}
	return FileCursor{File: l.currentPath, Offset: offset}, nil
}

func (l *FileLog) Iterate(ctx context.Context, opts ReadOptions, handler EventHandler) error {
	files, err := l.listFiles()
	if err != nil {
		return err
	}

	for _, p := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := l.iterateFile(ctx, p, opts, handler); err != nil {
			return err
		}
	}
	return nil
}

func (l *FileLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.writer != nil {
		if err := l.writer.Flush(); err != nil {
			return err
		}
	}
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

func (l *FileLog) ensureWriter(ts time.Time) error {
	day := ts.Format("20060102")
	if l.file != nil && l.currentDay == day {
		return nil
	}
	if err := os.MkdirAll(l.dir, 0o755); err != nil {
		return fmt.Errorf("create bus dir failed: %w", err)
	}
	if l.writer != nil {
		_ = l.writer.Flush()
	}
	if l.file != nil {
		_ = l.file.Close()
	}
	path := filepath.Join(l.dir, fmt.Sprintf("events-%s.log", day))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open bus log file failed: %w", err)
	}
	l.currentDay = day
	l.currentPath = path
	l.file = f
	l.writer = bufio.NewWriterSize(f, 64*1024)
	l.lastFlushTime = time.Time{}
	return nil
}

func (l *FileLog) listFiles() ([]string, error) {
	if err := os.MkdirAll(l.dir, 0o755); err != nil {
		return nil, fmt.Errorf("create bus dir failed: %w", err)
	}
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("read bus dir failed: %w", err)
	}
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "events-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		out = append(out, filepath.Join(l.dir, name))
	}
	sort.Strings(out)
	return out, nil
}

func (l *FileLog) iterateFile(ctx context.Context, path string, opts ReadOptions, handler EventHandler) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open bus log file failed: %w", err)
	}
	defer f.Close()

	reader := bufio.NewScanner(f)
	reader.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	offset := int64(0)
	for reader.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := reader.Bytes()
		cursor := FileCursor{File: path, Offset: offset}
		offset += int64(len(line) + 1)

		if opts.FromCursor != nil {
			if sameFilePath(path, opts.FromCursor.File) && cursor.Offset < opts.FromCursor.Offset {
				continue
			}
		}

		var ev BusEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			// Ignore malformed line for tail-truncate tolerance.
			continue
		}
		if !ShouldKeepEvent(ev, opts) {
			continue
		}
		if err := handler(ctx, ev, cursor); err != nil {
			return err
		}
	}
	if err := reader.Err(); err != nil {
		return fmt.Errorf("scan bus log file failed: %w", err)
	}
	return nil
}

func sameFilePath(a string, b string) bool {
	return filepath.Clean(a) == filepath.Clean(b)
}
