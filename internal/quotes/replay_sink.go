// replay_sink.go 是 replay.Service 和行情处理链之间的桥接层。
// 它把回放源里的 tick 事件重新还原成 tickEvent，再复用 mdSpi 和 marketDataRuntime 的实时处理逻辑。
package quotes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"ctp-future-kline/internal/bus"
	"ctp-future-kline/internal/config"
	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/replay"
	"ctp-future-kline/internal/strategy"
)

// ReplaySink 是 replay 事件到 quotes 行情处理器之间的桥接层。
//
// replay service 只负责把事件分发给 consumer，并不知道如何把 tick 真正写成分钟线。
// ReplaySink 的职责就是把 bus.BusEvent 还原成 tickEvent，再交给 mdSpi 复用实时链路逻辑。
type ReplaySink struct {
	mu               sync.Mutex
	store            *klineStore
	spi              *mdSpi
	seenFirstConsume map[string]struct{}
}

// NewReplaySink 为回放模式创建独立的 store、L9 计算器和 mdSpi。
func NewReplaySink(cfg config.CTPConfig, status *RuntimeStatusCenter) (*ReplaySink, error) {
	dbPath, err := resolveStoreDSN(cfg)
	if err != nil {
		return nil, err
	}
	store, err := newKlineStore(dbPath)
	if err != nil {
		return nil, err
	}
	var l9Calc *l9AsyncCalculator
	if cfg.IsL9AsyncEnabled() {
		l9Calc = newL9AsyncCalculator(store, true, 1, nil)
	}
	spi := newMdSpiWithStatusAndOptions(store, l9Calc, status, mdSpiOptions{
		tickDedupWindow:   time.Duration(cfg.TickDedupWindowSeconds) * time.Second,
		driftThreshold:    time.Duration(cfg.DriftThresholdSeconds) * time.Second,
		driftResumeTicks:  cfg.DriftResumeTicks,
		enableMultiMinute: cfg.IsMultiMinuteEnabled(),
		flowPath:          cfg.FlowPath,
		onTick: func(t tickEvent) {
			strategy.PublishReplayTick(strategy.TickEvent{
				InstrumentID:    t.InstrumentID,
				ExchangeID:      t.ExchangeID,
				ActionDay:       t.ActionDay,
				TradingDay:      t.TradingDay,
				UpdateTime:      t.UpdateTime,
				UpdateMillisec:  t.UpdateMillisec,
				ReceivedAt:      t.ReceivedAt,
				LastPrice:       t.LastPrice,
				Volume:          t.Volume,
				OpenInterest:    t.OpenInterest,
				SettlementPrice: t.SettlementPrice,
				BidPrice1:       t.BidPrice1,
				AskPrice1:       t.AskPrice1,
			})
		},
		onBar: func(bar minuteBar) {
			strategy.PublishReplayBar(strategy.BarEvent{
				Variety:         bar.Variety,
				InstrumentID:    bar.InstrumentID,
				Exchange:        bar.Exchange,
				DataTime:        bar.MinuteTime,
				AdjustedTime:    bar.AdjustedTime,
				Period:          bar.Period,
				Open:            bar.Open,
				High:            bar.High,
				Low:             bar.Low,
				Close:           bar.Close,
				Volume:          bar.Volume,
				OpenInterest:    bar.OpenInterest,
				SettlementPrice: bar.SettlementPrice,
			})
		},
		onPartialBar: func(bar minuteBar) {
			PublishChartPartialBar(bar, true)
		},
		onPersistTask: func(task persistTask) {
			PublishChartFinalBar(task.Bar, task.Replay)
		},
	})
	return &ReplaySink{
		store:            store,
		spi:              spi,
		seenFirstConsume: make(map[string]struct{}),
	}, nil
}

// ConsumeBusEvent 接收 replay service 分发来的 tick 事件。
// 处理顺序是：反序列化 payload -> 交给 mdSpi.ProcessReplayTick -> 复用完整聚合链路。
func (s *ReplaySink) ConsumeBusEvent(_ context.Context, ev bus.BusEvent) error {
	if s == nil || s.spi == nil || ev.Topic != bus.TopicTick {
		return nil
	}
	var tick tickEvent
	if err := json.Unmarshal(ev.Payload, &tick); err != nil {
		return fmt.Errorf("decode replay tick failed: %w", err)
	}
	// logger.Debug(
	// 	"replay sink received tick",
	// 	"task_id", ev.ReplayTaskID,
	// 	"event_id", ev.EventID,
	// 	"source", ev.Source,
	// 	"instrument_id", tick.InstrumentID,
	// 	"trading_day", tick.TradingDay,
	// 	"action_day", tick.ActionDay,
	// 	"update_time", tick.UpdateTime,
	// 	"update_millisec", tick.UpdateMillisec,
	// 	"received_at", tick.ReceivedAt.Format(time.RFC3339Nano),
	// 	"last_price", tick.LastPrice,
	// 	"volume", tick.Volume,
	// )
	s.logFirstConsume(ev.ReplayTaskID, tick)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.spi.ProcessReplayTick(tick); err != nil {
		logger.Error(
			"replay sink process tick failed",
			"task_id", ev.ReplayTaskID,
			"event_id", ev.EventID,
			"instrument_id", tick.InstrumentID,
			"error", err,
		)
		return err
	}
	// logger.Debug(
	// 	"replay sink processed tick",
	// 	"task_id", ev.ReplayTaskID,
	// 	"event_id", ev.EventID,
	// 	"instrument_id", tick.InstrumentID,
	// )
	return nil
}

// OnTaskFinished 在回放结束时强制 Flush，补落最后一根尚未因分钟切换而封口的 bar。
func (s *ReplaySink) OnTaskFinished(_ context.Context, snap replay.TaskSnapshot) error {
	if s == nil || s.spi == nil {
		return nil
	}
	if snap.Status != replay.StatusDone && snap.Status != replay.StatusStopped {
		s.resetReplayStageLogState()
		return nil
	}
	s.mu.Lock()
	err := s.spi.Flush()
	s.mu.Unlock()
	s.resetReplayStageLogState()
	return err
}

// Close 关闭回放模式专用的底层 store。
func (s *ReplaySink) Close() error {
	if s == nil || s.store == nil {
		return nil
	}
	return s.store.Close()
}

// resolveStoreDSN 解析回放使用的数据库连接信息。
func resolveStoreDSN(cfg config.CTPConfig) (string, error) {
	dsn := strings.TrimSpace(cfg.DBDSN)
	if dsn == "" {
		return "", fmt.Errorf("ctp.db dsn is required")
	}
	return dsn, nil
}

func (s *ReplaySink) logFirstConsume(taskID string, tick tickEvent) {
	instrumentID := strings.TrimSpace(tick.InstrumentID)
	if taskID == "" || instrumentID == "" {
		return
	}
	key := taskID + "|" + strings.ToLower(instrumentID)
	s.mu.Lock()
	if _, ok := s.seenFirstConsume[key]; ok {
		s.mu.Unlock()
		return
	}
	s.seenFirstConsume[key] = struct{}{}
	s.mu.Unlock()
	logger.Info(
		"replay sink first tick received for instrument",
		"stage", "ReplaySink.ConsumeBusEvent",
		"task_id", taskID,
		"instrument_id", instrumentID,
		"update_time", tick.UpdateTime,
		"update_millisec", tick.UpdateMillisec,
	)
}

func (s *ReplaySink) resetReplayStageLogState() {
	s.mu.Lock()
	s.seenFirstConsume = make(map[string]struct{})
	spi := s.spi
	s.mu.Unlock()
	if spi != nil {
		spi.ResetReplayStageLogState()
	}
}
