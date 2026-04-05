package importer

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"ctp-future-kline/internal/quotes"
)

var tradingSessionFileNamePattern = regexp.MustCompile(`(?i)^\d+#?[A-Za-z]+L9(?:[-_A-Za-z0-9]*)?\.txt$`)

type TradingSessionsImportResult struct {
	TotalFiles       int      `json:"total_files"`
	ProcessedFiles   int      `json:"processed_files"`
	TotalLines       int      `json:"total_lines"`
	InsertedRows     int      `json:"inserted_rows"`
	OverwrittenRows  int      `json:"overwritten_rows"`
	SkippedFiles     int      `json:"skipped_files"`
	ErrorCount       int      `json:"error_count"`
	UpdatedVarieties []string `json:"updated_varieties"`
	MarketDB         string   `json:"market_db"`
	SharedMetaDB     string   `json:"shared_meta_db"`
}

func ImportTradingSessions(id string, marketDBPath string, sharedDBPath string, files []UploadFile) (TradingSessionsImportResult, error) {
	filtered, skippedByName := filterTradingSessionFiles(files)
	if len(filtered) == 0 {
		return TradingSessionsImportResult{}, fmt.Errorf("no eligible L9 1m txt files found in selected directory")
	}
	session := NewTDXImportSessionWithOptions(id, marketDBPath, filtered, ImportOptions{
		SharedDBPath:        sharedDBPath,
		EnableMMRebuild:     false,
		AllowSessionInfer:   true,
		RequireL9:           true,
		OverwriteDuplicates: true,
		MaxDataDays:         5,
		SessionOnly:         true,
	}, noopHandler{})
	session.run()
	progress := session.Snapshot()
	if progress.ErrorCount > 0 && progress.LastError != "" {
		return TradingSessionsImportResult{}, fmt.Errorf("%s", progress.LastError)
	}
	if progress.TotalLines == 0 {
		if progress.LastError != "" {
			return TradingSessionsImportResult{}, fmt.Errorf("%s", progress.LastError)
		}
		return TradingSessionsImportResult{}, fmt.Errorf("no valid 1m rows parsed from selected L9 files")
	}
	varieties := collectImportedVarieties(filtered)
	return TradingSessionsImportResult{
		TotalFiles:       len(files),
		ProcessedFiles:   progress.ProcessedFiles,
		TotalLines:       progress.TotalLines,
		InsertedRows:     progress.InsertedRows,
		OverwrittenRows:  progress.OverwrittenRows,
		SkippedFiles:     progress.SkippedFiles + skippedByName,
		ErrorCount:       progress.ErrorCount,
		UpdatedVarieties: varieties,
		MarketDB:         marketDBPath,
		SharedMetaDB:     sharedDBPath,
	}, nil
}

func filterTradingSessionFiles(files []UploadFile) ([]UploadFile, int) {
	filtered := make([]UploadFile, 0, len(files))
	skipped := 0
	for _, file := range files {
		if !isTradingSessionCandidateFile(file.Name) {
			skipped++
			continue
		}
		filtered = append(filtered, file)
	}
	return filtered, skipped
}

func isTradingSessionCandidateFile(name string) bool {
	base := filepath.Base(strings.TrimSpace(name))
	if base == "" {
		return false
	}
	return tradingSessionFileNamePattern.MatchString(base)
}

func collectImportedVarieties(files []UploadFile) []string {
	set := make(map[string]struct{}, len(files))
	for _, file := range files {
		base := strings.TrimSpace(file.Name)
		if base == "" {
			continue
		}
		instrumentID := parseInstrumentIDFromHeaderLine(file.Data)
		if instrumentID == "" {
			parts := strings.SplitN(strings.TrimSuffix(base, filepathExt(base)), "#", 2)
			if len(parts) == 2 {
				instrumentID = parts[1]
			}
		}
		instrumentID = strings.ToLower(strings.TrimSpace(instrumentID))
		if strings.HasSuffix(instrumentID, "l9") {
			instrumentID = strings.TrimSuffix(instrumentID, "l9")
		}
		if v := quotes.NormalizeVariety(instrumentID); v != "" {
			set[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func filepathExt(name string) string {
	idx := strings.LastIndexByte(name, '.')
	if idx < 0 {
		return ""
	}
	return name[idx:]
}
