package replay

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/logger"
)

const (
	tickCSVSource = "replay.tickcsv"
)

// tickCSVEvent 表示从 CSV 中读出并标准化后的回放事件。
// 它同时保留 bus 事件本体、断点续播游标和排序用时间。
type tickCSVEvent struct {
	// Event 是标准化后的总线事件。
	Event bus.BusEvent
	// Cursor 是该事件在 tick CSV 源中的文件游标。
	Cursor bus.FileCursor
	// Time 是排序和推进回放时使用的事件时间。
	Time time.Time
	// InstrumentID 是该事件所属合约。
	InstrumentID string
}

// tickCSVLoadResult 汇总 tick 目录加载结果，供 replay service 初始化任务快照使用。
type tickCSVLoadResult struct {
	// Events 是加载并排序后的所有 tick 事件。
	Events []tickCSVEvent
	// FileCount 是扫描到的 tick CSV 文件数。
	FileCount int
	// InstrumentCount 是涉及的合约数量。
	InstrumentCount int
	// FirstTime 是最早事件时间。
	FirstTime *time.Time
	// LastTime 是最晚事件时间。
	LastTime *time.Time
}

// tickPayload 是写入 CSV 后再回放时恢复出来的 payload 结构。
type tickPayload struct {
	// InstrumentID 是合约代码。
	InstrumentID string `json:"InstrumentID"`
	// ExchangeID 是交易所代码。
	ExchangeID string `json:"ExchangeID"`
	// ActionDay 是自然日。
	ActionDay string `json:"ActionDay"`
	// TradingDay 是业务交易日。
	TradingDay string `json:"TradingDay"`
	// UpdateTime 是 HH:MM:SS 时间部分。
	UpdateTime string `json:"UpdateTime"`
	// UpdateMillisec 是毫秒部分。
	UpdateMillisec int `json:"UpdateMillisec"`
	// ReceivedAt 是录制该 tick 时的接收时间。
	ReceivedAt time.Time `json:"ReceivedAt"`
	// LastPrice 是最新价。
	LastPrice float64 `json:"LastPrice"`
	// Volume 是累计成交量。
	Volume int `json:"Volume"`
	// OpenInterest 是持仓量。
	OpenInterest float64 `json:"OpenInterest"`
	// SettlementPrice 是结算价。
	SettlementPrice float64 `json:"SettlementPrice"`
	// BidPrice1 是买一价。
	BidPrice1 float64 `json:"BidPrice1"`
	// AskPrice1 是卖一价。
	AskPrice1 float64 `json:"AskPrice1"`
}

// runTickDir 负责执行“从 tick CSV 目录回放”的主循环。
//
// 它会：
// 1. 扫描并加载目录内所有 CSV
// 2. 解析出统一的事件序列并按时间排序
// 3. 按 fast/realtime 模式推进回放时间
// 4. 把每一条事件交给 dispatch 分发到各个 consumer
func (s *Service) runTickDir(ctx context.Context, taskID string, req StartRequest, mode string, speed float64) {
	result, err := loadTickCSVEvents(req)
	if err != nil {
		s.mu.Lock()
		if s.snapshot.TaskID == taskID {
			s.snapshot.Status = StatusError
			s.snapshot.LastError = err.Error()
			s.snapshot.Errors++
			s.snapshot.FinishedAt = time.Now()
		}
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	if s.snapshot.TaskID == taskID {
		s.snapshot.TickFiles = result.FileCount
		s.snapshot.Instruments = result.InstrumentCount
		s.snapshot.TotalTicks = int64(len(result.Events))
		s.snapshot.FirstSimTime = result.FirstTime
		s.snapshot.LastSimTime = result.LastTime
	}
	s.mu.Unlock()

	var prevOccurred time.Time
	for _, item := range result.Events {
		if err := s.waitIfPaused(ctx, taskID); err != nil {
			break
		}
		if mode == "realtime" {
			if !prevOccurred.IsZero() && !item.Time.IsZero() {
				delta := item.Time.Sub(prevOccurred)
				if delta > 0 {
					wait := time.Duration(float64(delta) / speed)
					timer := time.NewTimer(wait)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
			}
			if !item.Time.IsZero() {
				prevOccurred = item.Time
			}
		}

		replayEvent := item.Event
		replayEvent.Replay = true
		replayEvent.ReplayTaskID = taskID

		dispatched, skipped, dispatchErr := s.dispatch(ctx, replayEvent)
		s.mu.Lock()
		if s.snapshot.TaskID == taskID {
			s.snapshot.ProcessedTicks++
			s.snapshot.Dispatched += dispatched
			s.snapshot.Skipped += skipped
			s.snapshot.CurrentInstrumentID = item.InstrumentID
			cur := item.Cursor
			s.snapshot.LastCursor = &cur
			if !item.Time.IsZero() {
				ts := item.Time
				s.snapshot.CurrentSimTime = &ts
			}
		}
		s.mu.Unlock()
		if dispatchErr != nil {
			s.mu.Lock()
			if s.snapshot.TaskID == taskID {
				s.snapshot.Status = StatusError
				s.snapshot.LastError = dispatchErr.Error()
				s.snapshot.Errors++
				s.snapshot.FinishedAt = time.Now()
			}
			s.mu.Unlock()
			return
		}
	}

	s.mu.Lock()
	if s.snapshot.TaskID == taskID && s.snapshot.Status == StatusStopped {
		s.snapshot.FinishedAt = time.Now()
	}
	s.mu.Unlock()
}

// loadTickCSVEvents 扫描 tick_dir 下所有 CSV，解析并合并成全局时间有序的事件列表。
func loadTickCSVEvents(req StartRequest) (tickCSVLoadResult, error) {
	dir := strings.TrimSpace(req.TickDir)
	if dir == "" {
		return tickCSVLoadResult{}, fmt.Errorf("tick_dir is required")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return tickCSVLoadResult{}, fmt.Errorf("read tick_dir failed: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.EqualFold(filepath.Ext(entry.Name()), ".csv") {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)

	events := make([]tickCSVEvent, 0, len(names)*64)
	instruments := make(map[string]struct{}, len(names))
	for _, name := range names {
		fileEvents, err := loadTickCSVFile(filepath.Join(dir, name), name, req)
		if err != nil {
			return tickCSVLoadResult{}, err
		}
		events = append(events, fileEvents...)
		for _, item := range fileEvents {
			if item.InstrumentID != "" {
				instruments[item.InstrumentID] = struct{}{}
			}
		}
	}

	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Time.Equal(events[j].Time) {
			if events[i].Cursor.File == events[j].Cursor.File {
				return events[i].Cursor.Offset < events[j].Cursor.Offset
			}
			return events[i].Cursor.File < events[j].Cursor.File
		}
		return events[i].Time.Before(events[j].Time)
	})
	var firstTime *time.Time
	var lastTime *time.Time
	if len(events) > 0 {
		if !events[0].Time.IsZero() {
			ts := events[0].Time
			firstTime = &ts
		}
		lastIdx := len(events) - 1
		if !events[lastIdx].Time.IsZero() {
			ts := events[lastIdx].Time
			lastTime = &ts
		}
	}
	return tickCSVLoadResult{
		Events:          events,
		FileCount:       len(names),
		InstrumentCount: len(instruments),
		FirstTime:       firstTime,
		LastTime:        lastTime,
	}, nil
}

// loadTickCSVFile 读取单个合约 CSV，并把每一行转换成可回放的 bus.BusEvent。
//
// 映射关系如下：
// 1. CSV 字段 -> tickPayload
// 2. tickPayload -> ev.Payload
// 3. ReceivedAt -> ev.OccurredAt / ev.ProducedAt
func loadTickCSVFile(path string, name string, req StartRequest) ([]tickCSVEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open tick csv failed: %s: %w", name, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1

	if _, err := reader.Read(); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("read tick csv header failed: %s: %w", name, err)
	}

	topics := bus.BuildSet(req.Topics)
	if len(topics) > 0 {
		if _, ok := topics[bus.TopicTick]; !ok {
			return nil, nil
		}
	}
	sources := bus.BuildSet(req.Sources)
	if len(sources) > 0 {
		if _, ok := sources[tickCSVSource]; !ok {
			return nil, nil
		}
	}

	out := make([]tickCSVEvent, 0, 256)
	firstReadLogged := false
	firstParsedLogged := false
	firstBusEventLogged := false
	lineNo := int64(1)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read tick csv record failed: %s:%d: %w", name, lineNo+1, err)
		}
		lineNo++
		if len(record) != 13 {
			return nil, fmt.Errorf("tick csv record malformed: %s:%d", name, lineNo)
		}
		if shouldSkipTickCSVRecord(name, lineNo, req.FromCursor) {
			continue
		}

		payload, occurredAt, err := parseTickCSVRecord(record)
		if err != nil {
			return nil, fmt.Errorf("parse tick csv record failed: %s:%d: %w", name, lineNo, err)
		}
		if !firstReadLogged {
			logger.Info(
				"replay tick csv first record loaded",
				"stage", "tick_csv",
				"file", name,
				"instrument_id", payload.InstrumentID,
				"line_no", lineNo,
			)
			firstReadLogged = true
		}
		if !firstParsedLogged {
			logger.Info(
				"replay tick csv first record parsed",
				"stage", "parseTickCSVRecord",
				"file", name,
				"instrument_id", payload.InstrumentID,
				"line_no", lineNo,
				"update_time", payload.UpdateTime,
				"update_millisec", payload.UpdateMillisec,
			)
			firstParsedLogged = true
		}
		if req.StartTime != nil && occurredAt.Before(*req.StartTime) {
			continue
		}
		if req.EndTime != nil && occurredAt.After(*req.EndTime) {
			continue
		}

		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal tick payload failed: %s:%d: %w", name, lineNo, err)
		}
		if !firstBusEventLogged {
			logger.Info(
				"replay bus event created for instrument",
				"stage", "BusEvent",
				"file", name,
				"instrument_id", payload.InstrumentID,
				"line_no", lineNo,
				"event_id", fmt.Sprintf("tickcsv:%s:%d", name, lineNo),
			)
			firstBusEventLogged = true
		}
		out = append(out, tickCSVEvent{
			Event: bus.BusEvent{
				EventID:    fmt.Sprintf("tickcsv:%s:%d", name, lineNo),
				Topic:      bus.TopicTick,
				Source:     tickCSVSource,
				OccurredAt: occurredAt,
				ProducedAt: payload.ReceivedAt,
				Payload:    raw,
			},
			Cursor: bus.FileCursor{
				File:   name,
				Offset: lineNo,
			},
			Time:         occurredAt,
			InstrumentID: strings.ToLower(payload.InstrumentID),
		})
	}
}

// shouldSkipTickCSVRecord 根据回放起点游标决定是否跳过当前行，用于断点续播。
func shouldSkipTickCSVRecord(name string, lineNo int64, cursor *bus.FileCursor) bool {
	if cursor == nil {
		return false
	}
	fileName := strings.TrimSpace(cursor.File)
	if fileName == "" {
		return lineNo < cursor.Offset
	}
	switch strings.Compare(name, fileName) {
	case -1:
		return true
	case 0:
		return lineNo < cursor.Offset
	default:
		return false
	}
}

// parseTickCSVRecord 把一行新格式 CSV 解析成 tickPayload，并返回该行用于排序和回放推进的时间。
func parseTickCSVRecord(record []string) (tickPayload, time.Time, error) {
	receivedAt, err := parseTickCSVTime(record[0], []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
	})
	if err != nil {
		return tickPayload{}, time.Time{}, fmt.Errorf("received_at: %w", err)
	}
	lastPrice, err := strconv.ParseFloat(strings.TrimSpace(record[6]), 64)
	if err != nil {
		return tickPayload{}, time.Time{}, fmt.Errorf("last_price: %w", err)
	}
	volume, err := strconv.Atoi(strings.TrimSpace(record[7]))
	if err != nil {
		return tickPayload{}, time.Time{}, fmt.Errorf("volume: %w", err)
	}
	openInterest, err := strconv.ParseFloat(strings.TrimSpace(record[8]), 64)
	if err != nil {
		return tickPayload{}, time.Time{}, fmt.Errorf("open_interest: %w", err)
	}
	settlementPrice, err := strconv.ParseFloat(strings.TrimSpace(record[9]), 64)
	if err != nil {
		return tickPayload{}, time.Time{}, fmt.Errorf("settlement_price: %w", err)
	}
	bidPrice1, err := strconv.ParseFloat(strings.TrimSpace(record[10]), 64)
	if err != nil {
		return tickPayload{}, time.Time{}, fmt.Errorf("bid_price1: %w", err)
	}
	askPrice1, err := strconv.ParseFloat(strings.TrimSpace(record[11]), 64)
	if err != nil {
		return tickPayload{}, time.Time{}, fmt.Errorf("ask_price1: %w", err)
	}
	updateMillisec, err := strconv.Atoi(strings.TrimSpace(record[12]))
	if err != nil {
		return tickPayload{}, time.Time{}, fmt.Errorf("update_millisec: %w", err)
	}

	payload := tickPayload{
		InstrumentID:    strings.TrimSpace(record[1]),
		ExchangeID:      strings.TrimSpace(record[2]),
		TradingDay:      strings.TrimSpace(record[3]),
		ActionDay:       strings.TrimSpace(record[4]),
		UpdateTime:      strings.TrimSpace(record[5]),
		ReceivedAt:      receivedAt,
		LastPrice:       lastPrice,
		Volume:          volume,
		OpenInterest:    openInterest,
		SettlementPrice: settlementPrice,
		BidPrice1:       bidPrice1,
		AskPrice1:       askPrice1,
		UpdateMillisec:  updateMillisec,
	}
	return payload, receivedAt, nil
}

// parseTickCSVTime 兼容多种时间布局，便于同时读取历史老文件和新录制文件。
func parseTickCSVTime(value string, layouts []string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	for _, layout := range layouts {
		ts, err := time.ParseInLocation(layout, value, time.Local)
		if err == nil {
			return ts, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time: %s", value)
}
