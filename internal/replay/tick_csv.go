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
	"unicode"

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

type TickCSVWindow struct {
	StartTime       *time.Time
	EndTime         *time.Time
	FileCount       int
	InstrumentCount int
	EventCount      int
}

// tickPayload 是写入 CSV 后再回放时恢复出来的 payload 结构。
type tickPayload struct {
	// InstrumentID 是合约代码。
	InstrumentID string `json:"InstrumentID"`
	// ExchangeID 是交易所代码。
	ExchangeID string `json:"ExchangeID"`
	// ExchangeInstID 是交易所合约代码。
	ExchangeInstID string `json:"ExchangeInstID"`
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
	// PreSettlementPrice 是昨结算价。
	PreSettlementPrice float64 `json:"PreSettlementPrice"`
	// PreClosePrice 是昨收盘价。
	PreClosePrice float64 `json:"PreClosePrice"`
	// PreOpenInterest 是昨持仓量。
	PreOpenInterest float64 `json:"PreOpenInterest"`
	// OpenPrice / HighestPrice / LowestPrice / ClosePrice / AveragePrice 是日内价格参考。
	OpenPrice    float64 `json:"OpenPrice"`
	HighestPrice float64 `json:"HighestPrice"`
	LowestPrice  float64 `json:"LowestPrice"`
	// Volume 是累计成交量。
	Volume int `json:"Volume"`
	// Turnover 是累计成交额。
	Turnover float64 `json:"Turnover"`
	// OpenInterest 是持仓量。
	OpenInterest float64 `json:"OpenInterest"`
	// ClosePrice 是今收盘价。
	ClosePrice float64 `json:"ClosePrice"`
	// SettlementPrice 是结算价。
	SettlementPrice float64 `json:"SettlementPrice"`
	// UpperLimitPrice / LowerLimitPrice 是涨跌停价。
	UpperLimitPrice float64 `json:"UpperLimitPrice"`
	LowerLimitPrice float64 `json:"LowerLimitPrice"`
	// AveragePrice 是均价。
	AveragePrice float64 `json:"AveragePrice"`
	// PreDelta / CurrDelta 保留原始 delta 字段。
	PreDelta  float64 `json:"PreDelta"`
	CurrDelta float64 `json:"CurrDelta"`
	// BidPrice1 是买一价。
	BidPrice1 float64 `json:"BidPrice1"`
	// AskPrice1 是卖一价。
	AskPrice1 float64 `json:"AskPrice1"`
	// BidVolume1 / AskVolume1 是买一量和卖一量。
	BidVolume1 int `json:"BidVolume1"`
	AskVolume1 int `json:"AskVolume1"`
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
				if err := s.waitRealtimeDelta(ctx, taskID, delta, speed); err != nil {
					return
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

func InspectTickCSVWindow(req StartRequest) (TickCSVWindow, error) {
	result, err := loadTickCSVEvents(req)
	if err != nil {
		return TickCSVWindow{}, err
	}
	return TickCSVWindow{
		StartTime:       result.FirstTime,
		EndTime:         result.LastTime,
		FileCount:       result.FileCount,
		InstrumentCount: result.InstrumentCount,
		EventCount:      len(result.Events),
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

	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("read tick csv header failed: %s: %w", name, err)
	}
	index := buildTickCSVHeaderIndex(header)

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
		if len(record) == 0 {
			continue
		}
		if shouldSkipTickCSVRecord(name, lineNo, req.FromCursor) {
			continue
		}

		payload, occurredAt, err := parseTickCSVRecord(record, index)
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
func buildTickCSVHeaderIndex(header []string) map[string]int {
	index := make(map[string]int, len(header))
	for i, item := range header {
		key := normalizeTickCSVHeaderKey(item)
		if key == "" {
			continue
		}
		index[key] = i
	}
	return index
}

func normalizeTickCSVHeaderKey(raw string) string {
	key := strings.TrimSpace(raw)
	if key == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(key) + 4)
	for i, r := range key {
		if r == ' ' || r == '-' {
			b.WriteByte('_')
			continue
		}
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				prev := rune(key[i-1])
				if prev != '_' && prev != ' ' && prev != '-' && !(prev >= 'A' && prev <= 'Z') {
					b.WriteByte('_')
				}
			}
			b.WriteRune(r + ('a' - 'A'))
			continue
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

func tickCSVString(record []string, index map[string]int, key string) string {
	if pos, ok := index[key]; ok && pos >= 0 && pos < len(record) {
		return strings.TrimSpace(record[pos])
	}
	return ""
}

func tickCSVFloat(record []string, index map[string]int, key string) (float64, error) {
	raw := tickCSVString(record, index, key)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return v, nil
}

func tickCSVInt(record []string, index map[string]int, key string) (int, error) {
	raw := tickCSVString(record, index, key)
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return v, nil
}

func parseTickCSVRecord(record []string, index map[string]int) (tickPayload, time.Time, error) {
	receivedAt, err := parseTickCSVTime(tickCSVString(record, index, "received_at"), []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
	})
	if err != nil {
		return tickPayload{}, time.Time{}, fmt.Errorf("received_at: %w", err)
	}
	lastPrice, err := tickCSVFloat(record, index, "last_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	preSettlementPrice, err := tickCSVFloat(record, index, "pre_settlement_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	preClosePrice, err := tickCSVFloat(record, index, "pre_close_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	preOpenInterest, err := tickCSVFloat(record, index, "pre_open_interest")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	openPrice, err := tickCSVFloat(record, index, "open_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	highestPrice, err := tickCSVFloat(record, index, "highest_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	lowestPrice, err := tickCSVFloat(record, index, "lowest_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	volume, err := tickCSVInt(record, index, "volume")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	turnover, err := tickCSVFloat(record, index, "turnover")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	openInterest, err := tickCSVFloat(record, index, "open_interest")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	closePrice, err := tickCSVFloat(record, index, "close_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	settlementPrice, err := tickCSVFloat(record, index, "settlement_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	upperLimitPrice, err := tickCSVFloat(record, index, "upper_limit_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	lowerLimitPrice, err := tickCSVFloat(record, index, "lower_limit_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	averagePrice, err := tickCSVFloat(record, index, "average_price")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	preDelta, err := tickCSVFloat(record, index, "pre_delta")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	currDelta, err := tickCSVFloat(record, index, "curr_delta")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	bidPrice1, err := tickCSVFloat(record, index, "bid_price1")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	askPrice1, err := tickCSVFloat(record, index, "ask_price1")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	updateMillisec, err := tickCSVInt(record, index, "update_millisec")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	bidVolume1, err := tickCSVInt(record, index, "bid_volume1")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}
	askVolume1, err := tickCSVInt(record, index, "ask_volume1")
	if err != nil {
		return tickPayload{}, time.Time{}, err
	}

	payload := tickPayload{
		InstrumentID:       tickCSVString(record, index, "instrument_id"),
		ExchangeID:         tickCSVString(record, index, "exchange_id"),
		ExchangeInstID:     tickCSVString(record, index, "exchange_inst_id"),
		TradingDay:         tickCSVString(record, index, "trading_day"),
		ActionDay:          tickCSVString(record, index, "action_day"),
		UpdateTime:         tickCSVString(record, index, "update_time"),
		ReceivedAt:         receivedAt,
		LastPrice:          lastPrice,
		PreSettlementPrice: preSettlementPrice,
		PreClosePrice:      preClosePrice,
		PreOpenInterest:    preOpenInterest,
		OpenPrice:          openPrice,
		HighestPrice:       highestPrice,
		LowestPrice:        lowestPrice,
		Volume:             volume,
		Turnover:           turnover,
		OpenInterest:       openInterest,
		ClosePrice:         closePrice,
		SettlementPrice:    settlementPrice,
		UpperLimitPrice:    upperLimitPrice,
		LowerLimitPrice:    lowerLimitPrice,
		AveragePrice:       averagePrice,
		PreDelta:           preDelta,
		CurrDelta:          currDelta,
		BidPrice1:          bidPrice1,
		AskPrice1:          askPrice1,
		UpdateMillisec:     updateMillisec,
		BidVolume1:         bidVolume1,
		AskVolume1:         askVolume1,
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
