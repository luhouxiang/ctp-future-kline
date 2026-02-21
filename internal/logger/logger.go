package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	logMu     sync.Mutex
	logFile   *os.File
	logWriter io.Writer = os.Stdout
	workDir             = mustGetwd()
)

func InitFile(path string) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	logMu.Lock()
	defer logMu.Unlock()
	if logFile != nil {
		_ = logFile.Close()
	}
	logFile = f
	logWriter = io.MultiWriter(os.Stdout, f)
	return nil
}

func Close() error {
	logMu.Lock()
	defer logMu.Unlock()
	if logFile == nil {
		return nil
	}
	err := logFile.Close()
	logFile = nil
	logWriter = os.Stdout
	return err
}

func Info(msg string, args ...any) {
	logWithCaller("INFO", msg, args...)
}

func Error(msg string, args ...any) {
	logWithCaller("ERROR", msg, args...)
}

func logWithCaller(level string, msg string, args ...any) {
	// Stack: caller -> logger.Info/Error -> logWithCaller
	source := "unknown:0"
	if _, file, line, ok := runtime.Caller(2); ok {
		source = fmt.Sprintf("%s:%d", shortPath(file), line)
	}
	timestamp := time.Now().Format("2006-01-02T15:04:05.000")

	var b strings.Builder
	b.Grow(128)
	b.WriteString("[")
	b.WriteString(timestamp)
	b.WriteString("][")
	b.WriteString(level)
	b.WriteString("]:")
	b.WriteString(source)
	b.WriteString(`|msg=`)
	b.WriteString(fmt.Sprintf("%q", msg))
	appendKeyValues(&b, args...)
	b.WriteByte('\n')

	logMu.Lock()
	defer logMu.Unlock()
	_, _ = io.WriteString(logWriter, b.String())
}

func shortPath(path string) string {
	cleaned := filepath.Clean(path)
	if workDir != "" {
		if rel, err := filepath.Rel(workDir, cleaned); err == nil {
			up := ".." + string(os.PathSeparator)
			if rel != "." && rel != ".." && !strings.HasPrefix(rel, up) {
				return filepath.ToSlash(rel)
			}
		}
	}

	normalized := filepath.ToSlash(cleaned)
	if idx := strings.Index(normalized, "/internal/"); idx >= 0 {
		return normalized[idx+1:]
	}
	if idx := strings.Index(normalized, "/cmd/"); idx >= 0 {
		return normalized[idx+1:]
	}
	return filepath.Base(cleaned)
}

func mustGetwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return wd
}

func appendKeyValues(b *strings.Builder, args ...any) {
	for i := 0; i < len(args); i += 2 {
		b.WriteByte(' ')
		key := fmt.Sprintf("%v", args[i])
		b.WriteString(key)
		b.WriteByte('=')
		if i+1 >= len(args) {
			b.WriteString("<missing>")
			continue
		}
		b.WriteString(formatValue(args[i+1]))
	}
}

func formatValue(v any) string {
	switch x := v.(type) {
	case string:
		if strings.ContainsAny(x, " \t\r\n\"=") {
			return fmt.Sprintf("%q", x)
		}
		return x
	case error:
		return fmt.Sprintf("%q", x.Error())
	default:
		return fmt.Sprintf("%v", x)
	}
}
