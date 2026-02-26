package trader

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type tickCSVRecorder struct {
	dir string

	mu      sync.Mutex
	writers map[string]*tickCSVFile
}

type tickCSVFile struct {
	file   *os.File
	writer *bufio.Writer
}

func newTickCSVRecorder(baseDir string) (*tickCSVRecorder, error) {
	dir := filepath.Join(strings.TrimSpace(baseDir), "ticks")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create tick csv dir failed: %w", err)
	}
	return &tickCSVRecorder{dir: dir, writers: make(map[string]*tickCSVFile)}, nil
}

func (r *tickCSVRecorder) Append(ev tickEvent) error {
	if r == nil {
		return nil
	}
	inst := strings.ToLower(strings.TrimSpace(ev.InstrumentID))
	if inst == "" {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	f, ok := r.writers[inst]
	if !ok {
		opened, err := r.openFileLocked(inst)
		if err != nil {
			return err
		}
		f = opened
		r.writers[inst] = f
	}

	line := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%.8f,%d,%.8f,%.8f,%.8f,%.8f,%d\n",
		ev.ReceivedAt.Format("2006-01-02 15:04:05.000"),
		ev.InstrumentID,
		ev.ExchangeID,
		ev.TradingDay,
		ev.ActionDay,
		ev.UpdateTime,
		ev.AdjustedTickTime.Format("2006-01-02 15:04:05"),
		ev.LastPrice,
		ev.Volume,
		ev.OpenInterest,
		ev.SettlementPrice,
		ev.BidPrice1,
		ev.AskPrice1,
		ev.UpdateMillisec,
	)
	if _, err := f.writer.WriteString(line); err != nil {
		return fmt.Errorf("write tick csv failed: %w", err)
	}
	if err := f.writer.Flush(); err != nil {
		return fmt.Errorf("flush tick csv failed: %w", err)
	}
	return nil
}

func (r *tickCSVRecorder) openFileLocked(instrumentID string) (*tickCSVFile, error) {
	name := sanitizeTickFileName(instrumentID) + ".csv"
	path := filepath.Join(r.dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open tick csv failed: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat tick csv failed: %w", err)
	}
	writer := bufio.NewWriter(file)
	if stat.Size() == 0 {
		header := "received_at,instrument_id,exchange_id,trading_day,action_day,update_time,adjusted_tick_time,last_price,volume,open_interest,settlement_price,bid_price1,ask_price1,update_millisec\n"
		if _, err := writer.WriteString(header); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("write tick csv header failed: %w", err)
		}
		if err := writer.Flush(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("flush tick csv header failed: %w", err)
		}
	}
	return &tickCSVFile{file: file, writer: writer}, nil
}

func sanitizeTickFileName(v string) string {
	s := strings.ToLower(strings.TrimSpace(v))
	if s == "" {
		return "unknown"
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
			b.WriteRune(ch)
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}
