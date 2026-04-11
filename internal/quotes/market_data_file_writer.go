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
	"ctp-future-kline/internal/queuewatch"
)

type fileFlushRequest struct {
	done chan error
}

type shardFileWriter struct {
	shardID       int
	status        *RuntimeStatusCenter
	dir           string
	in            chan any
	queueHandle   *queuewatch.QueueHandle
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
	queueCfg := queuewatch.DefaultConfig("")
	registry := (*queuewatch.Registry)(nil)
	if status != nil && status.QueueRegistry() != nil {
		registry = status.QueueRegistry()
		queueCfg = registry.Config()
	}
	w := &shardFileWriter{
		shardID:       shardID,
		status:        status,
		dir:           dir,
		in:            make(chan any, queueCfg.FilePerShardCapacity),
		flushInterval: 500 * time.Millisecond,
		fsyncInterval: 5 * time.Second,
		files:         make(map[string]*tickCSVFile),
	}
	if registry != nil {
		w.queueHandle = registry.Register(queuewatch.QueueSpec{
			Name:        fmt.Sprintf("tick_file_writer_%02d", shardID),
			Category:    "quotes_sidecar",
			Criticality: "best_effort",
			Capacity:    queueCfg.FilePerShardCapacity,
			LossPolicy:  "best_effort",
			BasisText:   fmt.Sprintf("?? shard ? tick ???????? %d?", queueCfg.FilePerShardCapacity),
		})
		w.queueHandle.ObserveDepth(len(w.in))
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
		if w.queueHandle != nil {
			w.queueHandle.MarkEnqueued(len(w.in))
		}
	default:
		if w.queueHandle != nil {
			w.queueHandle.MarkDropped(len(w.in))
		}
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
			if w.queueHandle != nil {
				w.queueHandle.MarkDequeued(len(w.in))
			}
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

	line := formatTickCSVLine(tickEvent{
		InstrumentID:       ev.InstrumentID,
		ExchangeID:         ev.ExchangeID,
		ExchangeInstID:     ev.ExchangeInstID,
		TradingDay:         tradingDay,
		UpdateTime:         ev.UpdateTime,
		ReceivedAt:         ev.ReceivedAt,
		LastPrice:          ev.LastPrice,
		PreSettlementPrice: ev.PreSettlementPrice,
		PreClosePrice:      ev.PreClosePrice,
		PreOpenInterest:    ev.PreOpenInterest,
		OpenPrice:          ev.OpenPrice,
		HighestPrice:       ev.HighestPrice,
		LowestPrice:        ev.LowestPrice,
		Volume:             ev.Volume,
		Turnover:           ev.Turnover,
		OpenInterest:       ev.OpenInterest,
		ClosePrice:         ev.ClosePrice,
		SettlementPrice:    ev.SettlementPrice,
		UpperLimitPrice:    ev.UpperLimitPrice,
		LowerLimitPrice:    ev.LowerLimitPrice,
		AveragePrice:       ev.AveragePrice,
		PreDelta:           ev.PreDelta,
		CurrDelta:          ev.CurrDelta,
		BidPrice1:          ev.BidPrice1,
		AskPrice1:          ev.AskPrice1,
		UpdateMillisec:     ev.UpdateMillisec,
		BidVolume1:         ev.BidVolume1,
		AskVolume1:         ev.AskVolume1,
	}, actionDay)
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
		header := "received_at,instrument_id,exchange_id,exchange_inst_id,trading_day,action_day,update_time,last_price,pre_settlement_price,pre_close_price,pre_open_interest,open_price,highest_price,lowest_price,volume,turnover,open_interest,close_price,settlement_price,upper_limit_price,lower_limit_price,average_price,pre_delta,curr_delta,bid_price1,ask_price1,update_millisec,bid_volume1,ask_volume1\n"
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
