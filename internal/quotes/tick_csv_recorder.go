// tick_csv_recorder.go 负责把接收到的原始 tick 按合约录制成 CSV。
// 这些文件主要用于排障、数据核对和 replay.tickcsv 回放，不参与业务聚合决策。
package quotes

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// tickCSVRecorder 负责把实时收到的 tick 追加保存成按合约拆分的 CSV。
//
// 它不参与任何业务聚合，只承担“把实时输入原样记录下来，供后续回放复现”这一个职责。
type tickCSVRecorder struct {
	dir string

	mu            sync.Mutex
	writers       map[string]*tickCSVFile
	flushEvery    int
	flushInterval time.Duration
}

// tickCSVFile 表示单个合约对应的 CSV 文件句柄和带缓冲写入器。
type tickCSVFile struct {
	file         *os.File
	writer       *bufio.Writer
	pendingLines int
	lastFlush    time.Time
}

// newTickCSVRecorder 在 flow 目录下创建 ticks 子目录，并准备按合约拆分写文件。
func newTickCSVRecorder(baseDir string) (*tickCSVRecorder, error) {
	dir := filepath.Join(strings.TrimSpace(baseDir), "ticks")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create tick csv dir failed: %w", err)
	}
	return &tickCSVRecorder{
		dir:           dir,
		writers:       make(map[string]*tickCSVFile),
		flushEvery:    128,
		flushInterval: time.Second,
	}, nil
}

// Append 把一笔实时 tick 追加到对应合约的 CSV 文件中。
// CSV 只保存原始行情字段与 ReceivedAt，业务时间在聚合时再计算。
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

	line := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%d,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%d,%d,%d\n",
		ev.ReceivedAt.Format("2006-01-02 15:04:05.000"),
		ev.InstrumentID,
		ev.ExchangeID,
		ev.ExchangeInstID,
		ev.TradingDay,
		ev.ActionDay,
		ev.UpdateTime,
		ev.LastPrice,
		ev.PreSettlementPrice,
		ev.PreClosePrice,
		ev.PreOpenInterest,
		ev.OpenPrice,
		ev.HighestPrice,
		ev.LowestPrice,
		ev.Volume,
		ev.Turnover,
		ev.OpenInterest,
		ev.ClosePrice,
		ev.SettlementPrice,
		ev.UpperLimitPrice,
		ev.LowerLimitPrice,
		ev.AveragePrice,
		ev.PreDelta,
		ev.CurrDelta,
		ev.BidPrice1,
		ev.AskPrice1,
		ev.UpdateMillisec,
		ev.BidVolume1,
		ev.AskVolume1,
	)
	if _, err := f.writer.WriteString(line); err != nil {
		return fmt.Errorf("write tick csv failed: %w", err)
	}
	f.pendingLines++
	if r.shouldFlushLocked(f, ev.ReceivedAt) {
		if err := f.writer.Flush(); err != nil {
			return fmt.Errorf("flush tick csv failed: %w", err)
		}
		f.pendingLines = 0
		f.lastFlush = ev.ReceivedAt
	}
	return nil
}

func (r *tickCSVRecorder) Close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var firstErr error
	for instrumentID, f := range r.writers {
		if f.writer != nil {
			if err := f.writer.Flush(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("flush tick csv for %s failed: %w", instrumentID, err)
			}
		}
		if f.file != nil {
			if err := f.file.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("close tick csv for %s failed: %w", instrumentID, err)
			}
		}
	}
	r.writers = make(map[string]*tickCSVFile)
	return firstErr
}

func (r *tickCSVRecorder) shouldFlushLocked(f *tickCSVFile, ts time.Time) bool {
	if f == nil {
		return false
	}
	if r.flushEvery <= 1 || f.pendingLines >= r.flushEvery {
		return true
	}
	if r.flushInterval <= 0 {
		return false
	}
	if ts.IsZero() {
		ts = time.Now()
	}
	if f.lastFlush.IsZero() {
		return false
	}
	return ts.Sub(f.lastFlush) >= r.flushInterval
}

// openFileLocked 打开某个合约对应的 CSV 文件。
// 如果文件是首次创建，会写入新格式表头。
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
		header := "received_at,instrument_id,exchange_id,exchange_inst_id,trading_day,action_day,update_time,last_price,pre_settlement_price,pre_close_price,pre_open_interest,open_price,highest_price,lowest_price,volume,turnover,open_interest,close_price,settlement_price,upper_limit_price,lower_limit_price,average_price,pre_delta,curr_delta,bid_price1,ask_price1,update_millisec,bid_volume1,ask_volume1\n"
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

// sanitizeTickFileName 把合约名规整成安全的文件名。
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
