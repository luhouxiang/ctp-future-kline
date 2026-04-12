package quotes

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"ctp-future-kline/internal/logger"
)

const tickArchiveCutoffHour = 16

func ArchiveTickDirOnStartup(baseDir string, now time.Time) error {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	if now.Hour() < tickArchiveCutoffHour {
		return nil
	}
	tickDir := filepath.Join(baseDir, "ticks")
	info, err := os.Stat(tickDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat tick dir failed: %w", err)
	}
	if !info.IsDir() {
		return nil
	}

	tradingDay, fileCount, err := detectTickDirTradingDay(tickDir)
	if err != nil {
		return err
	}
	if fileCount == 0 || tradingDay == "" {
		return nil
	}

	archiveDir := filepath.Join(baseDir, "ticks-"+tradingDay)
	archiveZip := archiveDir + ".zip"
	if _, err := os.Stat(archiveDir); err == nil {
		return fmt.Errorf("tick archive target dir already exists: %s", archiveDir)
	}
	if _, err := os.Stat(archiveZip); err == nil {
		logger.Info("tick archive skipped", "reason", "archive_zip_already_exists", "trading_day", tradingDay, "archive_zip", archiveZip, "tick_dir", tickDir, "file_count", fileCount)
		return nil
	}

	if err := os.Rename(tickDir, archiveDir); err != nil {
		return fmt.Errorf("rename tick dir failed: %w", err)
	}
	success := false
	defer func() {
		if success {
			return
		}
		if _, err := os.Stat(archiveDir); err == nil {
			_ = os.Rename(archiveDir, tickDir)
		}
	}()

	if err := zipDirWithRoot(archiveDir, archiveZip); err != nil {
		return err
	}
	if err := os.RemoveAll(archiveDir); err != nil {
		return fmt.Errorf("remove archived tick dir failed: %w", err)
	}
	success = true
	logger.Info("tick archive completed", "tick_dir", tickDir, "archive_zip", archiveZip, "trading_day", tradingDay, "file_count", fileCount)
	return nil
}

func detectTickDirTradingDay(dir string) (string, int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", 0, fmt.Errorf("read tick dir failed: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".csv") {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	if len(names) == 0 {
		return "", 0, nil
	}

	daySet := make(map[string]struct{}, len(names))
	for _, name := range names {
		tradingDay, err := readTickCSVTradingDay(filepath.Join(dir, name))
		if err != nil {
			return "", 0, fmt.Errorf("read tick trading day failed: %s: %w", name, err)
		}
		if tradingDay == "" {
			continue
		}
		daySet[tradingDay] = struct{}{}
	}
	if len(daySet) == 0 {
		return "", len(names), nil
	}
	if len(daySet) > 1 {
		days := make([]string, 0, len(daySet))
		for day := range daySet {
			days = append(days, day)
		}
		sort.Strings(days)
		return "", len(names), fmt.Errorf("multiple trading days found in tick dir: %s", strings.Join(days, ","))
	}
	for day := range daySet {
		return day, len(names), nil
	}
	return "", len(names), nil
}

func readTickCSVTradingDay(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open tick csv failed: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return "", nil
		}
		return "", fmt.Errorf("read tick csv header failed: %w", err)
	}
	index := buildTickCSVArchiveHeaderIndex(header)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("read tick csv record failed: %w", err)
		}
		day := strings.TrimSpace(tickCSVArchiveString(record, index, "trading_day"))
		if day != "" {
			return day, nil
		}
	}
}

func buildTickCSVArchiveHeaderIndex(header []string) map[string]int {
	index := make(map[string]int, len(header))
	for i, item := range header {
		key := normalizeTickCSVArchiveHeaderKey(item)
		if key == "" {
			continue
		}
		index[key] = i
	}
	return index
}

func normalizeTickCSVArchiveHeaderKey(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(raw))
	for _, ch := range raw {
		switch {
		case ch >= 'A' && ch <= 'Z':
			b.WriteByte(byte(ch + ('a' - 'A')))
		case ch == ' ' || ch == '-':
			b.WriteByte('_')
		default:
			b.WriteRune(ch)
		}
	}
	return b.String()
}

func tickCSVArchiveString(record []string, index map[string]int, key string) string {
	pos, ok := index[key]
	if !ok || pos < 0 || pos >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[pos])
}

func zipDirWithRoot(srcDir string, destZip string) error {
	out, err := os.OpenFile(destZip, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create tick archive zip failed: %w", err)
	}
	defer out.Close()

	zw := zip.NewWriter(out)

	root := filepath.Base(srcDir)
	err = filepath.Walk(srcDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		zipName := filepath.ToSlash(filepath.Join(root, rel))
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Name = zipName
		header.Method = zip.Deflate
		writer, err := zw.CreateHeader(header)
		if err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		if _, err := io.Copy(writer, in); err != nil {
			_ = in.Close()
			return err
		}
		if err := in.Close(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("zip tick dir failed: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("close tick archive zip failed: %w", err)
	}
	return nil
}
