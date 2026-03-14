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
	logMu      sync.Mutex
	levelFiles map[string]*os.File
	baseFile   *os.File
	logWriters = map[string]io.Writer{
		"DEBUG": os.Stdout,
		"INFO":  os.Stdout,
		"WARN":  os.Stdout,
		"ERROR": os.Stdout,
	}
	minLevel = levelInfo
	workDir  = mustGetwd()
)

const (
	levelDebug = iota
	levelInfo
	levelWarn
	levelError
)

func InitFile(path string) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	paths := buildLevelLogPaths(path)
	files := make(map[string]*os.File, len(paths))
	base, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	for level, p := range paths {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			_ = base.Close()
			for _, opened := range files {
				_ = opened.Close()
			}
			return err
		}
		files[level] = f
	}

	logMu.Lock()
	defer logMu.Unlock()
	for _, f := range levelFiles {
		_ = f.Close()
	}
	if baseFile != nil {
		_ = baseFile.Close()
	}
	levelFiles = files
	baseFile = base
	logWriters = map[string]io.Writer{
		"DEBUG": io.MultiWriter(os.Stdout, base, files["DEBUG"]),
		"INFO":  io.MultiWriter(os.Stdout, base, files["INFO"]),
		"WARN":  io.MultiWriter(os.Stdout, base, files["WARN"]),
		"ERROR": io.MultiWriter(os.Stdout, base, files["ERROR"]),
	}
	return nil
}

func SetLevel(level string) error {
	parsed, err := parseLevel(level)
	if err != nil {
		return err
	}
	logMu.Lock()
	defer logMu.Unlock()
	minLevel = parsed
	return nil
}

func Close() error {
	logMu.Lock()
	defer logMu.Unlock()
	if len(levelFiles) == 0 {
		return nil
	}
	var firstErr error
	for _, f := range levelFiles {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if baseFile != nil {
		if err := baseFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	levelFiles = nil
	baseFile = nil
	logWriters = map[string]io.Writer{
		"DEBUG": os.Stdout,
		"INFO":  os.Stdout,
		"WARN":  os.Stdout,
		"ERROR": os.Stdout,
	}
	return firstErr
}

func Debug(msg string, args ...any) {
	logWithCaller("DEBUG", msg, args...)
}

func Info(msg string, args ...any) {
	logWithCaller("INFO", msg, args...)
}

func Warn(msg string, args ...any) {
	logWithCaller("WARN", msg, args...)
}

func Error(msg string, args ...any) {
	logWithCaller("ERROR", msg, args...)
}

func logWithCaller(level string, msg string, args ...any) {
	if !shouldLog(level) {
		return
	}
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
	w := logWriters[level]
	if w == nil {
		w = os.Stdout
	}
	_, _ = io.WriteString(w, b.String())
}

func shouldLog(level string) bool {
	logMu.Lock()
	defer logMu.Unlock()
	current, err := parseLevel(level)
	if err != nil {
		return false
	}
	return current >= minLevel
}

func parseLevel(level string) (int, error) {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "DEBUG":
		return levelDebug, nil
	case "", "INFO":
		return levelInfo, nil
	case "WARN":
		return levelWarn, nil
	case "ERROR":
		return levelError, nil
	default:
		return 0, fmt.Errorf("invalid log level: %s", level)
	}
}

func buildLevelLogPaths(path string) map[string]string {
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(filepath.Base(path), ext)
	dir := filepath.Dir(path)
	if base == "" {
		base = "server"
	}
	if ext == "" {
		ext = ".log"
	}
	return map[string]string{
		"DEBUG": filepath.Join(dir, base+".debug"+ext),
		"INFO":  filepath.Join(dir, base+".info"+ext),
		"WARN":  filepath.Join(dir, base+".warn"+ext),
		"ERROR": filepath.Join(dir, base+".error"+ext),
	}
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
