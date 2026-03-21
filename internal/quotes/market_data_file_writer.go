// market_data_file_writer.go 负责行情处理链路中的文件侧输出。
// 它按 shard 异步落盘调试/追踪文件，避免文件 I/O 直接阻塞实时 tick 处理线程。
package quotes

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"ctp-future-kline/internal/logger"
)

type fileFlushRequest struct {
	done chan error
}

type shardFileWriter struct {
	shardID       int
	status        *RuntimeStatusCenter
	dir           string
	in            chan any
	flushInterval time.Duration
	fsyncInterval time.Duration
	files         map[string]*tickCSVFile
}

func newShardFileWriter(baseDir string, shardID int, status *RuntimeStatusCenter) (*shardFileWriter, error) {
	if strings.TrimSpace(baseDir) == "" {
		return nil, nil
	}
	dir := filepath.Join(strings.TrimSpace(baseDir), "ticks")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &shardFileWriter{
		shardID:       shardID,
		status:        status,
		dir:           dir,
		in:            make(chan any, defaultFileQueueCapacity/defaultMarketDataShardCount+1),
		flushInterval: 500 * time.Millisecond,
		fsyncInterval: 5 * time.Second,
		files:         make(map[string]*tickCSVFile),
	}
	go w.run()
	return w, nil
}

func (w *shardFileWriter) Enqueue(ev tickEvent) {
	if w == nil {
		return
	}
	select {
	case w.in <- ev:
	default:
		logger.Warn("tick file queue full, dropping event", "shard_id", w.shardID, "instrument_id", ev.InstrumentID)
	}
}

func (w *shardFileWriter) Flush() error {
	if w == nil {
		return nil
	}
	done := make(chan error, 1)
	w.in <- fileFlushRequest{done: done}
	return <-done
}

func (w *shardFileWriter) run() {
	ticker := time.NewTicker(w.flushInterval)
	fsyncTicker := time.NewTicker(w.fsyncInterval)
	defer ticker.Stop()
	defer fsyncTicker.Stop()

	for {
		select {
		case msg := <-w.in:
			switch v := msg.(type) {
			case tickEvent:
				if err := w.append(v); err != nil {
					logger.Error("append shard tick csv failed", "shard_id", w.shardID, "instrument_id", v.InstrumentID, "error", err)
				}
			case fileFlushRequest:
				v.done <- w.flush(false)
			}
		case <-ticker.C:
			if err := w.flush(false); err != nil {
				logger.Error("flush shard tick csv failed", "shard_id", w.shardID, "error", err)
			}
		case <-fsyncTicker.C:
			if err := w.flush(true); err != nil {
				logger.Error("fsync shard tick csv failed", "shard_id", w.shardID, "error", err)
			}
		}
	}
}

func (w *shardFileWriter) append(ev tickEvent) error {
	inst := strings.ToLower(strings.TrimSpace(ev.InstrumentID))
	if inst == "" {
		return nil
	}
	f, ok := w.files[inst]
	if !ok {
		opened, err := w.openFile(inst)
		if err != nil {
			return err
		}
		f = opened
		w.files[inst] = f
	}

	tradingDay := ev.TradingDay
	actionDay := ev.ActionDay
	if strings.TrimSpace(ev.RawActionDay) != "" {
		actionDay = ev.RawActionDay
	}

	line := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%.8f,%d,%.8f,%.8f,%.8f,%.8f,%d\n",
		ev.ReceivedAt.Format("2006-01-02 15:04:05.000"),
		ev.InstrumentID,
		ev.ExchangeID,
		tradingDay,
		actionDay,
		ev.UpdateTime,
		ev.LastPrice,
		ev.Volume,
		ev.OpenInterest,
		ev.SettlementPrice,
		ev.BidPrice1,
		ev.AskPrice1,
		ev.UpdateMillisec,
	)
	if _, err := f.writer.WriteString(line); err != nil {
		return err
	}
	f.pendingLines++
	return nil
}

func (w *shardFileWriter) flush(syncDisk bool) error {
	startedAt := time.Now()
	for instrumentID, f := range w.files {
		if f.writer == nil || f.pendingLines == 0 {
			continue
		}
		if err := f.writer.Flush(); err != nil {
			return fmt.Errorf("flush tick csv for %s failed: %w", instrumentID, err)
		}
		f.pendingLines = 0
		f.lastFlush = startedAt
		if syncDisk && f.file != nil {
			if err := f.file.Sync(); err != nil {
				return fmt.Errorf("sync tick csv for %s failed: %w", instrumentID, err)
			}
		}
	}
	if w.status != nil {
		w.status.MarkFileFlush(int(time.Since(startedAt).Milliseconds()), len(w.in))
	}
	return nil
}

func (w *shardFileWriter) openFile(instrumentID string) (*tickCSVFile, error) {
	name := sanitizeTickFileName(instrumentID) + ".csv"
	path := filepath.Join(w.dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	writer := bufio.NewWriter(file)
	if stat.Size() == 0 {
		header := "received_at,instrument_id,exchange_id,trading_day,action_day,update_time,last_price,volume,open_interest,settlement_price,bid_price1,ask_price1,update_millisec\n"
		if _, err := writer.WriteString(header); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := writer.Flush(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return &tickCSVFile{file: file, writer: writer}, nil
}
