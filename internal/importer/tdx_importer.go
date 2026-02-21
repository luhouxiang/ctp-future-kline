package importer

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	dbx "ctp-go-demo/internal/db"
	"ctp-go-demo/internal/klineclock"
	"ctp-go-demo/internal/logger"
	"ctp-go-demo/internal/trader"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

const (
	ActionOverwrite = "overwrite"
	ActionSkip      = "skip"
	ActionCancel    = "cancel"
)

var tdxFileNamePattern = regexp.MustCompile(`^\d+#.+\.txt$`)

var exchangeCodeMap = map[string]string{
	"28": "CZCE",
	"29": "DCE",
	"30": "SHFE",
	"47": "CFFEX",
	"66": "GFEX",
}

type UploadFile struct {
	Name string
	Data []byte
}

type Progress struct {
	TotalFiles      int    `json:"total_files"`
	ProcessedFiles  int    `json:"processed_files"`
	TotalLines      int    `json:"total_lines"`
	InsertedRows    int    `json:"inserted_rows"`
	OverwrittenRows int    `json:"overwritten_rows"`
	SkippedRows     int    `json:"skipped_rows"`
	SkippedFiles    int    `json:"skipped_files"`
	ErrorCount      int    `json:"error_count"`
	Done            bool   `json:"done"`
	Canceled        bool   `json:"canceled"`
	LastError       string `json:"last_error,omitempty"`
}

type ConflictRecord struct {
	FileName       string           `json:"file_name"`
	LineNumber     int              `json:"line_number"`
	InstrumentID   string           `json:"instrument_id"`
	Exchange       string           `json:"exchange"`
	MinuteTime     string           `json:"minute_time"`
	Existing       trader.MinuteBar `json:"existing"`
	Incoming       trader.MinuteBar `json:"incoming"`
	OverwriteScope string           `json:"overwrite_scope"`
	Batch          bool             `json:"batch"`
	NewCount       int              `json:"new_count"`
	DuplicateCount int              `json:"duplicate_count"`
}

type DecisionRequest struct {
	Action                string `json:"action"`
	OverwriteInstrument   bool   `json:"overwrite_instrument"`
	OverwriteAllContracts bool   `json:"overwrite_all_contracts"`
}

type EventHandler interface {
	OnProgress(Progress)
	OnConflict(ConflictRecord)
	OnDone(Progress)
	OnError(error)
}

type noopHandler struct{}

func (noopHandler) OnProgress(Progress)       {}
func (noopHandler) OnConflict(ConflictRecord) {}
func (noopHandler) OnDone(Progress)           {}
func (noopHandler) OnError(error)             {}

type TDXImportSession struct {
	id      string
	dbPath  string
	files   []UploadFile
	handler EventHandler

	mu            sync.Mutex
	progress      Progress
	globalReplace bool
	replaceByInst map[string]bool
	pending       *ConflictRecord
	decisionCh    chan DecisionRequest
}

type rawTdxRow struct {
	lineNumber   int
	tradingDay   time.Time
	hhmm         int
	open         float64
	high         float64
	low          float64
	closePrice   float64
	volume       int64
	openInterest float64
	settlement   float64
}

func NewTDXImportSession(id string, dbPath string, files []UploadFile, handler EventHandler) *TDXImportSession {
	if handler == nil {
		handler = noopHandler{}
	}
	return &TDXImportSession{
		id:            id,
		dbPath:        dbPath,
		files:         files,
		handler:       handler,
		progress:      Progress{TotalFiles: len(files)},
		replaceByInst: make(map[string]bool),
		decisionCh:    make(chan DecisionRequest, 1),
	}
}

func (s *TDXImportSession) ID() string {
	return s.id
}

func (s *TDXImportSession) Snapshot() Progress {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.progress
}

func (s *TDXImportSession) Start() {
	logger.Info("import session goroutine start", "session_id", s.id, "file_count", len(s.files))
	go s.run()
}

func (s *TDXImportSession) ApplyDecision(req DecisionRequest) error {
	switch req.Action {
	case ActionOverwrite, ActionSkip, ActionCancel:
	default:
		return fmt.Errorf("invalid action: %s", req.Action)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		return errors.New("no pending conflict")
	}
	select {
	case s.decisionCh <- req:
		return nil
	default:
		return errors.New("decision channel busy")
	}
}

func (s *TDXImportSession) run() {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("import session panic recovered", "session_id", s.id, "panic", r, "stack", string(debug.Stack()))
			s.fail(fmt.Errorf("import session panic: %v", r))
		}
	}()
	logger.Info("import run begin", "session_id", s.id, "file_count", len(s.files))

	var db *sql.DB
	var err error
	openStart := time.Now()
	openDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-openDone:
				return
			case <-ticker.C:
				logger.Info("import open store waiting", "session_id", s.id, "elapsed_ms", time.Since(openStart).Milliseconds())
			}
		}
	}()
	err = withSQLiteBusyRetry(func() error {
		var openErr error
		db, openErr = openImportDB(s.dbPath)
		return openErr
	})
	close(openDone)
	if err != nil {
		logger.Error("import open store failed", "session_id", s.id, "error", err)
		s.fail(err)
		return
	}
	logger.Info("import open store success", "session_id", s.id)

	for _, f := range s.files {
		if s.isDone() {
			logger.Info("import session terminated early", "session_id", s.id)
			break
		}
		currentFile := filepath.Base(strings.TrimSpace(f.Name))
		logger.Info("import processing file", "session_id", s.id, "file", currentFile)
		if err := s.processFile(db, f); err != nil {
			s.recordError(err)
		} else {
			logger.Info("import file processed", "session_id", s.id, "file", currentFile)
		}
		s.mu.Lock()
		s.progress.ProcessedFiles++
		p := s.progress
		s.mu.Unlock()
		s.handler.OnProgress(p)
	}
	_ = db.Close()

	s.mu.Lock()
	s.progress.Done = true
	final := s.progress
	s.mu.Unlock()
	logger.Info("import run finished", "session_id", s.id, "processed_files", final.ProcessedFiles, "total_lines", final.TotalLines, "error_count", final.ErrorCount)
	s.handler.OnDone(final)
}

func openImportDB(path string) (*sql.DB, error) {
	logger.Info("import db open begin", "db_path", path)
	db, err := dbx.Open(path)
	if err != nil {
		logger.Error("import db open failed", "db_path", path, "error", err)
		return nil, fmt.Errorf("open mysql failed: %w", err)
	}
	logger.Info("import db open success", "db_path", path)
	return db, nil
}

func (s *TDXImportSession) processFile(db *sql.DB, f UploadFile) error {
	base := filepath.Base(strings.TrimSpace(f.Name))
	if !tdxFileNamePattern.MatchString(base) {
		s.incSkippedFile()
		logger.Info("import file skipped: name pattern mismatch", "session_id", s.id, "file", base)
		return nil
	}
	logger.Info("import file begin", "session_id", s.id, "file", base, "size_bytes", len(f.Data))
	parts := strings.SplitN(strings.TrimSuffix(base, filepath.Ext(base)), "#", 2)
	if len(parts) != 2 {
		s.incSkippedFile()
		logger.Info("import file skipped: invalid file name format", "session_id", s.id, "file", base)
		return nil
	}
	exchange, ok := exchangeCodeMap[parts[0]]
	if !ok {
		s.incSkippedFile()
		logger.Error("import file rejected: unsupported exchange", "session_id", s.id, "file", base, "exchange_code", parts[0])
		return fmt.Errorf("unsupported exchange code %s for file %s", parts[0], base)
	}
	instrumentID := strings.ToUpper(strings.TrimSpace(parts[1]))
	if parsed := parseInstrumentIDFromHeaderLine(f.Data); parsed != "" {
		instrumentID = parsed
	}
	isL9 := strings.HasSuffix(instrumentID, "L9")

	text, err := decodeGBK(f.Data)
	if err != nil {
		return fmt.Errorf("decode gbk failed for %s: %w", base, err)
	}
	lines := splitLines(text)
	if len(lines) < 3 {
		s.incSkippedFile()
		return nil
	}

	timeResolver := klineclock.NewCalendarResolver(db)
	rawRows := make([]rawTdxRow, 0, len(lines))
	daySet := make(map[string]time.Time)
	for i, line := range lines {
		if s.isDone() {
			return nil
		}
		line = strings.TrimSpace(line)
		if line == "" || i <= 1 {
			continue
		}
		if strings.HasPrefix(line, "#数据来源") {
			break
		}
		row, ok, err := parseRawDataLine(line, i+1)
		if err != nil {
			return fmt.Errorf("parse line failed in %s:%d: %w", base, i+1, err)
		}
		if !ok {
			continue
		}
		rawRows = append(rawRows, row)
		day := normalizeDay(row.tradingDay)
		daySet[day.Format("2006-01-02")] = day
	}

	fileTradingDays := make([]time.Time, 0, len(daySet))
	for _, day := range daySet {
		fileTradingDays = append(fileTradingDays, day)
	}
	sort.Slice(fileTradingDays, func(i, j int) bool {
		return fileTradingDays[i].Before(fileTradingDays[j])
	})

	bars := make([]trader.MinuteBar, 0, len(rawRows))
	for _, row := range rawRows {
		bar, err := buildMinuteBarFromRawRow(row, instrumentID, exchange, fileTradingDays, timeResolver)
		if err != nil {
			return fmt.Errorf("parse line failed in %s:%d: %w", base, row.lineNumber, err)
		}
		bars = append(bars, bar)
	}

	s.mu.Lock()
	s.progress.TotalLines += len(bars)
	p := s.progress
	s.mu.Unlock()
	s.handler.OnProgress(p)

	if len(bars) == 0 {
		s.incSkippedFile()
		logger.Info("import file parsed", "session_id", s.id, "file", base, "parsed_rows", 0)
		return nil
	}
	logger.Info("import file parsed", "session_id", s.id, "file", base, "parsed_rows", len(bars))
	before := s.Snapshot()
	if err := s.batchCompareAndWrite(db, base, bars, isL9); err != nil {
		afterErr := s.Snapshot()
		logger.Error("import file write failed",
			"session_id", s.id,
			"file", base,
			"parsed_rows", len(bars),
			"attempt_inserted_rows", afterErr.InsertedRows-before.InsertedRows,
			"attempt_overwritten_rows", afterErr.OverwrittenRows-before.OverwrittenRows,
			"attempt_skipped_rows", afterErr.SkippedRows-before.SkippedRows,
			"error", err,
		)
		return err
	}
	after := s.Snapshot()
	logger.Info("import file write result",
		"session_id", s.id,
		"file", base,
		"parsed_rows", len(bars),
		"inserted_rows", after.InsertedRows-before.InsertedRows,
		"overwritten_rows", after.OverwrittenRows-before.OverwrittenRows,
		"skipped_rows", after.SkippedRows-before.SkippedRows,
	)
	return nil
}

func (s *TDXImportSession) batchCompareAndWrite(db *sql.DB, fileName string, bars []trader.MinuteBar, isL9 bool) error {
	var tableName string
	var err error
	if isL9 {
		tableName, err = trader.TableNameForL9Variety(bars[0].Variety)
	} else {
		tableName, err = trader.TableNameForVariety(bars[0].Variety)
	}
	if err != nil {
		return err
	}
	if err := withSQLiteBusyRetry(func() error { return ensureKlineTable(db, tableName) }); err != nil {
		return err
	}

	storedInstrumentID := strings.ToLower(strings.TrimSpace(bars[0].InstrumentID))
	var dupSet map[string]bool
	err = withSQLiteBusyRetry(func() error {
		var qErr error
		dupSet, qErr = loadExistingTimeSet(db, tableName, storedInstrumentID, bars[0].Exchange, bars[0].Period, bars)
		return qErr
	})
	if err != nil {
		return err
	}

	newBars := make([]trader.MinuteBar, 0, len(bars))
	dupBars := make([]trader.MinuteBar, 0, len(bars))
	for _, bar := range bars {
		key := bar.MinuteTime.Format("2006-01-02 15:04:00")
		if dupSet[key] {
			dupBars = append(dupBars, bar)
		} else {
			newBars = append(newBars, bar)
		}
	}

	shouldOverwriteDup := false
	s.mu.Lock()
	shouldOverwriteDup = s.globalReplace || s.replaceByInst[storedInstrumentID]
	s.mu.Unlock()
	logger.Info("import file dedup split",
		"session_id", s.id,
		"file", fileName,
		"instrument_id", storedInstrumentID,
		"total_rows", len(bars),
		"new_rows", len(newBars),
		"duplicate_rows", len(dupBars),
		"overwrite_enabled", shouldOverwriteDup,
	)

	if len(dupBars) > 0 && !shouldOverwriteDup {
		record := ConflictRecord{
			FileName:       fileName,
			LineNumber:     0,
			InstrumentID:   storedInstrumentID,
			Exchange:       bars[0].Exchange,
			MinuteTime:     "",
			Batch:          true,
			NewCount:       len(newBars),
			DuplicateCount: len(dupBars),
		}
		if len(dupBars) > 0 {
			record.Incoming = dupBars[0]
		}

		s.mu.Lock()
		s.pending = &record
		s.mu.Unlock()
		s.handler.OnConflict(record)

		decision := <-s.decisionCh
		s.mu.Lock()
		s.pending = nil
		if decision.OverwriteAllContracts {
			s.globalReplace = true
		}
		if decision.OverwriteInstrument {
			s.replaceByInst[storedInstrumentID] = true
		}
		s.mu.Unlock()

		switch decision.Action {
		case ActionCancel:
			s.mu.Lock()
			s.progress.Canceled = true
			s.progress.Done = true
			p := s.progress
			s.mu.Unlock()
			s.handler.OnProgress(p)
			return nil
		case ActionSkip:
			if err := withSQLiteBusyRetry(func() error { return upsertBarsInTx(db, tableName, newBars) }); err != nil {
				return err
			}
			s.mu.Lock()
			s.progress.InsertedRows += len(newBars)
			s.progress.SkippedRows += len(dupBars)
			p := s.progress
			s.mu.Unlock()
			s.handler.OnProgress(p)
			return nil
		case ActionOverwrite:
			if err := withSQLiteBusyRetry(func() error { return upsertBarsInTx(db, tableName, bars) }); err != nil {
				return err
			}
			s.mu.Lock()
			s.progress.InsertedRows += len(newBars)
			s.progress.OverwrittenRows += len(dupBars)
			p := s.progress
			s.mu.Unlock()
			s.handler.OnProgress(p)
			return nil
		default:
			return fmt.Errorf("unknown action: %s", decision.Action)
		}
	}

	// Auto mode (global or instrument overwrite enabled), or no duplicates.
	if shouldOverwriteDup {
		if err := withSQLiteBusyRetry(func() error { return upsertBarsInTx(db, tableName, bars) }); err != nil {
			return err
		}
		s.mu.Lock()
		s.progress.InsertedRows += len(newBars)
		s.progress.OverwrittenRows += len(dupBars)
		p := s.progress
		s.mu.Unlock()
		s.handler.OnProgress(p)
		return nil
	}

	if err := withSQLiteBusyRetry(func() error { return upsertBarsInTx(db, tableName, newBars) }); err != nil {
		return err
	}
	s.mu.Lock()
	s.progress.InsertedRows += len(newBars)
	p := s.progress
	s.mu.Unlock()
	s.handler.OnProgress(p)
	return nil
}

func (s *TDXImportSession) isDone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.progress.Done || s.progress.Canceled
}

func (s *TDXImportSession) incSkippedFile() {
	s.mu.Lock()
	s.progress.SkippedFiles++
	s.mu.Unlock()
}

func (s *TDXImportSession) fail(err error) {
	s.mu.Lock()
	s.progress.ErrorCount++
	s.progress.LastError = err.Error()
	s.progress.Done = true
	p := s.progress
	s.mu.Unlock()
	logger.Error("import session failed", "session_id", s.id, "error", err, "error_count", p.ErrorCount)
	s.handler.OnError(err)
	s.handler.OnDone(p)
}

func (s *TDXImportSession) recordError(err error) {
	s.mu.Lock()
	s.progress.ErrorCount++
	s.progress.LastError = err.Error()
	p := s.progress
	s.mu.Unlock()
	logger.Error("import file process failed", "session_id", s.id, "error", err, "processed_files", p.ProcessedFiles, "total_lines", p.TotalLines)
	s.handler.OnError(err)
	s.handler.OnProgress(p)
}

func decodeGBK(data []byte) (string, error) {
	reader := transform.NewReader(bytes.NewReader(data), simplifiedchinese.GBK.NewDecoder())
	out, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func splitLines(text string) []string {
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	lines := make([]string, 0, 4096)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines
}

func parseRawDataLine(line string, lineNumber int) (rawTdxRow, bool, error) {
	fields := strings.Split(line, ",")
	if len(fields) != 9 {
		return rawTdxRow{}, false, nil
	}
	datePart := strings.TrimSpace(fields[0])
	timePart := strings.TrimSpace(fields[1])
	if len(timePart) < 4 {
		timePart = strings.Repeat("0", 4-len(timePart)) + timePart
	}
	if len(timePart) > 4 {
		timePart = timePart[:4]
	}
	hhmm, err := strconv.Atoi(timePart)
	if err != nil {
		return rawTdxRow{}, false, err
	}
	tradingDay, err := time.ParseInLocation("2006/01/02", datePart, time.Local)
	if err != nil {
		return rawTdxRow{}, false, err
	}

	open, err := strconv.ParseFloat(strings.TrimSpace(fields[2]), 64)
	if err != nil {
		return rawTdxRow{}, false, err
	}
	high, err := strconv.ParseFloat(strings.TrimSpace(fields[3]), 64)
	if err != nil {
		return rawTdxRow{}, false, err
	}
	low, err := strconv.ParseFloat(strings.TrimSpace(fields[4]), 64)
	if err != nil {
		return rawTdxRow{}, false, err
	}
	closePrice, err := strconv.ParseFloat(strings.TrimSpace(fields[5]), 64)
	if err != nil {
		return rawTdxRow{}, false, err
	}
	volume, err := strconv.ParseInt(strings.TrimSpace(fields[6]), 10, 64)
	if err != nil {
		return rawTdxRow{}, false, err
	}
	openInterest, err := strconv.ParseFloat(strings.TrimSpace(fields[7]), 64)
	if err != nil {
		return rawTdxRow{}, false, err
	}
	settlement, err := strconv.ParseFloat(strings.TrimSpace(fields[8]), 64)
	if err != nil {
		return rawTdxRow{}, false, err
	}

	return rawTdxRow{
		lineNumber:   lineNumber,
		tradingDay:   normalizeDay(tradingDay),
		hhmm:         hhmm,
		open:         open,
		high:         high,
		low:          low,
		closePrice:   closePrice,
		volume:       volume,
		openInterest: openInterest,
		settlement:   settlement,
	}, true, nil
}

func buildMinuteBarFromRawRow(
	row rawTdxRow,
	instrumentID string,
	exchange string,
	fileTradingDays []time.Time,
	resolver *klineclock.CalendarResolver,
) (trader.MinuteBar, error) {
	minuteTime, adjustedTime, err := buildBarTimesWithFileDays(row.tradingDay, row.hhmm, fileTradingDays, resolver)
	if err != nil {
		return trader.MinuteBar{}, err
	}
	instrumentID = strings.ToLower(strings.TrimSpace(instrumentID))
	varietySource := instrumentID
	if strings.HasSuffix(varietySource, "l9") {
		varietySource = strings.TrimSuffix(varietySource, "l9")
	}
	return trader.MinuteBar{
		Variety:         trader.NormalizeVariety(varietySource),
		InstrumentID:    instrumentID,
		Exchange:        exchange,
		MinuteTime:      minuteTime.Truncate(time.Minute),
		AdjustedTime:    adjustedTime.Truncate(time.Minute),
		Period:          "1m",
		Open:            row.open,
		High:            row.high,
		Low:             row.low,
		Close:           row.closePrice,
		Volume:          row.volume,
		OpenInterest:    row.openInterest,
		SettlementPrice: row.settlement,
	}, nil
}

func buildBarTimesWithFileDays(
	tradingDay time.Time,
	hhmm int,
	fileTradingDays []time.Time,
	resolver *klineclock.CalendarResolver,
) (time.Time, time.Time, error) {
	if hhmm < 0 || hhmm > 2359 {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid hhmm: %d", hhmm)
	}
	hour := hhmm / 100
	minute := hhmm % 100
	if hour > 23 || minute > 59 {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid hhmm: %d", hhmm)
	}

	tradingDay = normalizeDay(tradingDay)
	dataTime := time.Date(tradingDay.Year(), tradingDay.Month(), tradingDay.Day(), hour, minute, 0, 0, time.Local)
	if hhmm >= 800 && hhmm <= 1600 {
		return dataTime, dataTime, nil
	}

	prev, ok := resolvePrevTradingDayFromFile(fileTradingDays, tradingDay)
	if !ok {
		if resolver == nil {
			return time.Time{}, time.Time{}, fmt.Errorf("nil calendar resolver")
		}
		var err error
		prev, err = resolver.PrevTradingDay(tradingDay)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
	}

	adjDate := prev
	if hhmm < 800 {
		adjDate = prev.AddDate(0, 0, 1)
	}
	adjusted := time.Date(adjDate.Year(), adjDate.Month(), adjDate.Day(), hour, minute, 0, 0, time.Local)
	return dataTime, adjusted, nil
}

func resolvePrevTradingDayFromFile(days []time.Time, current time.Time) (time.Time, bool) {
	if len(days) == 0 {
		return time.Time{}, false
	}
	current = normalizeDay(current)
	idx := sort.Search(len(days), func(i int) bool {
		return !normalizeDay(days[i]).Before(current)
	})
	if idx <= 0 {
		return time.Time{}, false
	}
	return normalizeDay(days[idx-1]), true
}

func normalizeDay(day time.Time) time.Time {
	return time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.Local)
}

func loadExistingTimeSet(db *sql.DB, tableName, instrumentID, exchange, period string, bars []trader.MinuteBar) (map[string]bool, error) {
	minT := bars[0].MinuteTime
	maxT := bars[0].MinuteTime
	for _, bar := range bars[1:] {
		if bar.MinuteTime.Before(minT) {
			minT = bar.MinuteTime
		}
		if bar.MinuteTime.After(maxT) {
			maxT = bar.MinuteTime
		}
	}
	timeColumn, err := resolveDataTimeColumn(db, tableName)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`
SELECT "%s"
FROM "%s"
WHERE "InstrumentID" = ?
  AND "Exchange" = ?
  AND "Period" = ?
  AND "%s" >= ?
  AND "%s" <= ?`, timeColumn, tableName, timeColumn, timeColumn)
	rows, err := db.Query(
		query,
		strings.ToLower(strings.TrimSpace(instrumentID)),
		exchange,
		period,
		minT.Format("2006-01-02 15:04:00"),
		maxT.Format("2006-01-02 15:04:00"),
	)
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return map[string]bool{}, nil
		}
		return nil, fmt.Errorf("query existing keys failed: %w", err)
	}
	defer rows.Close()
	out := make(map[string]bool, len(bars))
	for rows.Next() {
		var ts time.Time
		if err := rows.Scan(&ts); err != nil {
			return nil, fmt.Errorf("scan existing key failed: %w", err)
		}
		out[ts.Format("2006-01-02 15:04:00")] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing keys failed: %w", err)
	}
	return out, nil
}

func ensureKlineTable(db *sql.DB, tableName string) error {
	stmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS "%s" (
  "%s" VARCHAR(32) NOT NULL,
  "%s" VARCHAR(16) NOT NULL,
  "%s" DATETIME NOT NULL,
  "%s" DATETIME NOT NULL,
  "%s" VARCHAR(8) NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" BIGINT NOT NULL,
  "%s" DOUBLE NOT NULL,
  "%s" DOUBLE NOT NULL,
  PRIMARY KEY ("%s", "%s", "%s", "%s")
)`,
		tableName,
		trader.ColInstrumentID,
		trader.ColExchange,
		trader.ColTime,
		trader.ColAdjustedTime,
		trader.ColPeriod,
		trader.ColOpen,
		trader.ColHigh,
		trader.ColLow,
		trader.ColClose,
		trader.ColVolume,
		trader.ColOpenInterest,
		trader.ColSettlement,
		trader.ColTime, trader.ColInstrumentID, trader.ColExchange, trader.ColPeriod,
	)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("ensure kline table failed: %w", err)
	}
	if err := ensureDataTimeColumn(db, tableName); err != nil {
		return err
	}
	if _, err := db.Exec(fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN "%s" DATETIME`, tableName, trader.ColAdjustedTime)); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		// modernc sqlite returns "duplicate column name"
		if !strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
			return fmt.Errorf("add AdjustedTime column failed: %w", err)
		}
	}
	if _, err := db.Exec(fmt.Sprintf(`UPDATE "%s" SET "%s"="%s" WHERE "%s" IS NULL`,
		tableName, trader.ColAdjustedTime, trader.ColTime, trader.ColAdjustedTime)); err != nil {
		return fmt.Errorf("backfill AdjustedTime failed: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf(`CREATE INDEX "idx_%s_inst_period_adj" ON "%s"("%s","%s","%s" DESC)`,
		tableName, tableName, trader.ColInstrumentID, trader.ColPeriod, trader.ColAdjustedTime)); err != nil && !isDuplicateIndexErr(err) {
		return fmt.Errorf("create adjusted index failed: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf(`CREATE INDEX "idx_%s_adj" ON "%s"("%s" DESC)`,
		tableName, tableName, trader.ColAdjustedTime)); err != nil && !isDuplicateIndexErr(err) {
		return fmt.Errorf("create adjusted index failed: %w", err)
	}
	return nil
}

func ensureDataTimeColumn(db *sql.DB, tableName string) error {
	hasDataTime, err := tableHasColumn(db, tableName, trader.ColDataTime)
	if err != nil {
		return err
	}
	if hasDataTime {
		return nil
	}
	hasLegacyTime, err := tableHasColumn(db, tableName, "Time")
	if err != nil {
		return err
	}
	if !hasLegacyTime {
		return fmt.Errorf("table %s missing both %s and Time columns", tableName, trader.ColDataTime)
	}
	if _, err := db.Exec(fmt.Sprintf(`ALTER TABLE "%s" RENAME COLUMN "Time" TO "%s"`, tableName, trader.ColDataTime)); err != nil {
		return fmt.Errorf("rename Time to %s failed: %w", trader.ColDataTime, err)
	}
	return nil
}

func resolveDataTimeColumn(db *sql.DB, tableName string) (string, error) {
	hasDataTime, err := tableHasColumn(db, tableName, trader.ColDataTime)
	if err != nil {
		return "", err
	}
	if hasDataTime {
		return trader.ColDataTime, nil
	}
	hasLegacyTime, err := tableHasColumn(db, tableName, "Time")
	if err != nil {
		return "", err
	}
	if hasLegacyTime {
		return "Time", nil
	}
	return "", fmt.Errorf("table %s missing both %s and Time columns", tableName, trader.ColDataTime)
}

func tableHasColumn(db *sql.DB, tableName string, column string) (bool, error) {
	rows, err := db.Query(`
SELECT column_name
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = ?`, tableName)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}
	return false, rows.Err()
}

func upsertBarsInTx(db *sql.DB, tableName string, bars []trader.MinuteBar) error {
	if len(bars) == 0 {
		return nil
	}
	if err := ensureKlineTable(db, tableName); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	stmt := fmt.Sprintf(`
INSERT INTO "%s"
("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s"),
  "%s" = VALUES("%s")`,
		tableName,
		trader.ColInstrumentID, trader.ColExchange, trader.ColTime, trader.ColAdjustedTime, trader.ColPeriod, trader.ColOpen, trader.ColHigh, trader.ColLow, trader.ColClose, trader.ColVolume, trader.ColOpenInterest, trader.ColSettlement,
		trader.ColAdjustedTime, trader.ColAdjustedTime,
		trader.ColOpen, trader.ColOpen,
		trader.ColHigh, trader.ColHigh,
		trader.ColLow, trader.ColLow,
		trader.ColClose, trader.ColClose,
		trader.ColVolume, trader.ColVolume,
		trader.ColOpenInterest, trader.ColOpenInterest,
		trader.ColSettlement, trader.ColSettlement,
	)
	prep, err := tx.Prepare(stmt)
	if err != nil {
		return fmt.Errorf("prepare upsert failed: %w", err)
	}
	defer prep.Close()

	for _, bar := range bars {
		storedInstrumentID := strings.ToLower(strings.TrimSpace(bar.InstrumentID))
		if storedInstrumentID == "" {
			return fmt.Errorf("invalid instrument id %q for table %q", bar.InstrumentID, tableName)
		}
		if _, err := prep.Exec(
			storedInstrumentID,
			bar.Exchange,
			bar.MinuteTime.Format("2006-01-02 15:04:00"),
			chooseAdjustedTime(bar).Format("2006-01-02 15:04:00"),
			bar.Period,
			bar.Open,
			bar.High,
			bar.Low,
			bar.Close,
			bar.Volume,
			bar.OpenInterest,
			bar.SettlementPrice,
		); err != nil {
			return fmt.Errorf("exec upsert failed: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upsert tx failed: %w", err)
	}
	return nil
}

func parseInstrumentIDFromHeaderLine(data []byte) string {
	text, err := decodeGBK(data)
	if err != nil {
		return ""
	}
	lines := splitLines(text)
	if len(lines) == 0 {
		return ""
	}
	head := strings.TrimSpace(lines[0])
	if head == "" {
		return ""
	}
	fields := strings.FieldsFunc(head, func(r rune) bool {
		return r == ' ' || r == '\t' || r == ','
	})
	if len(fields) == 0 {
		return ""
	}
	return strings.ToUpper(strings.TrimSpace(fields[0]))
}

func chooseAdjustedTime(bar trader.MinuteBar) time.Time {
	if !bar.AdjustedTime.IsZero() {
		return bar.AdjustedTime
	}
	return bar.MinuteTime
}

func withSQLiteBusyRetry(fn func() error) error {
	const maxAttempts = 8
	baseDelay := 200 * time.Millisecond
	maxDelay := 3 * time.Second
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !isSQLiteBusyError(err) || attempt == maxAttempts-1 {
			if isSQLiteBusyError(err) {
				logger.Error("mysql lock retry exhausted", "attempt", attempt+1, "max_attempts", maxAttempts, "error", err)
			}
			return err
		}
		delay := baseDelay * time.Duration(1<<attempt)
		if delay > maxDelay {
			delay = maxDelay
		}
		jitter := time.Duration(rand.Int63n(int64(delay / 5)))
		logger.Info("mysql lock retry", "attempt", attempt+1, "max_attempts", maxAttempts, "wait_ms", (delay + jitter).Milliseconds(), "error", err)
		time.Sleep(delay + jitter)
	}
	return nil
}

func isSQLiteBusyError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "lock wait timeout") ||
		strings.Contains(msg, "deadlock found") ||
		strings.Contains(msg, "error 1205") ||
		strings.Contains(msg, "error 1213")
}

func isDuplicateIndexErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "already exists")
}
