// market_data_side_effects.go 负责实时行情的旁路分发。
// 它把 tick/bar 事件异步推送到策略模块、总线日志等外部消费者，并记录 side effect 阶段的队列延迟。
package quotes

import (
	"sync"
	"time"

	"ctp-future-kline/internal/logger"
	"ctp-future-kline/internal/queuewatch"
)

type marketDataSideEffects struct {
	tickCh chan tickEvent
	barCh  chan minuteBar

	tickQueue *queuewatch.QueueHandle
	barQueue  *queuewatch.QueueHandle
	onTick    func(tickEvent)
	onBar     func(minuteBar)
	status    *RuntimeStatusCenter

	wg sync.WaitGroup

	mu             sync.Mutex
	lastTickDropAt time.Time
	lastBarDropAt  time.Time
}

func newMarketDataSideEffects(status *RuntimeStatusCenter, onTick func(tickEvent), onBar func(minuteBar)) *marketDataSideEffects {
	queueCfg := queuewatch.DefaultConfig("")
	registry := (*queuewatch.Registry)(nil)
	if status != nil && status.QueueRegistry() != nil {
		registry = status.QueueRegistry()
		queueCfg = registry.Config()
	}
	s := &marketDataSideEffects{
		tickCh: make(chan tickEvent, queueCfg.SideEffectTickCapacity),
		barCh:  make(chan minuteBar, queueCfg.SideEffectBarCapacity),
		onTick: onTick,
		onBar:  onBar,
		status: status,
	}
	if registry != nil {
		s.tickQueue = registry.Register(queuewatch.QueueSpec{
			Name:        "side_effect_tick",
			Category:    "quotes_sidecar",
			Criticality: "best_effort",
			Capacity:    queueCfg.SideEffectTickCapacity,
			LossPolicy:  "best_effort",
			BasisText:   "tick ???????????/????????????",
		})
		s.barQueue = registry.Register(queuewatch.QueueSpec{
			Name:        "side_effect_bar",
			Category:    "quotes_sidecar",
			Criticality: "best_effort",
			Capacity:    queueCfg.SideEffectBarCapacity,
			LossPolicy:  "best_effort",
			BasisText:   "bar ???????????/????????????",
		})
	}
	if onTick != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for ev := range s.tickCh {
				if s.tickQueue != nil {
					s.tickQueue.MarkDequeued(len(s.tickCh))
				}
				now := time.Now()
				queueMS := 0.0
				if !ev.SideEffectEnqueuedAt.IsZero() {
					queueMS = now.Sub(ev.SideEffectEnqueuedAt).Seconds() * 1000
				}
				if s.status != nil {
					s.status.MarkSideEffectLatency("tick", ev.InstrumentID, queueMS)
				}
				if queueMS >= latencyLogThreshold.Seconds()*1000 {
					logger.Warn("market data tick side effect delayed", "instrument_id", ev.InstrumentID, "queue_ms", queueMS)
				}
				ev.SideEffectHandledAt = now
				s.onTick(ev)
			}
		}()
	}
	if onBar != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for ev := range s.barCh {
				if s.barQueue != nil {
					s.barQueue.MarkDequeued(len(s.barCh))
				}
				now := time.Now()
				queueMS := 0.0
				if !ev.SideEffectEnqueuedAt.IsZero() {
					queueMS = now.Sub(ev.SideEffectEnqueuedAt).Seconds() * 1000
				}
				if s.status != nil {
					s.status.MarkSideEffectLatency("bar", ev.InstrumentID, queueMS)
				}
				if queueMS >= latencyLogThreshold.Seconds()*1000 {
					logger.Warn("market data bar side effect delayed", "instrument_id", ev.InstrumentID, "queue_ms", queueMS)
				}
				s.onBar(ev)
			}
		}()
	}
	return s
}

func (s *marketDataSideEffects) PublishTick(ev tickEvent) {
	if s == nil || s.onTick == nil {
		return
	}
	select {
	case s.tickCh <- ev:
		if s.tickQueue != nil {
			s.tickQueue.MarkEnqueued(len(s.tickCh))
		}
	default:
		if s.tickQueue != nil {
			s.tickQueue.MarkDropped(len(s.tickCh))
		}
		s.logDropLocked("tick", ev.InstrumentID)
	}
}

func (s *marketDataSideEffects) PublishBar(ev minuteBar) {
	if s == nil || s.onBar == nil {
		return
	}
	select {
	case s.barCh <- ev:
		if s.barQueue != nil {
			s.barQueue.MarkEnqueued(len(s.barCh))
		}
	default:
		if s.barQueue != nil {
			s.barQueue.MarkDropped(len(s.barCh))
		}
		s.logDropLocked("bar", ev.InstrumentID)
	}
}

func (s *marketDataSideEffects) Close() {
	if s == nil {
		return
	}
	if s.onTick != nil {
		close(s.tickCh)
	}
	if s.onBar != nil {
		close(s.barCh)
	}
	s.wg.Wait()
}

func (s *marketDataSideEffects) logDropLocked(kind string, instrumentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	switch kind {
	case "tick":
		if now.Sub(s.lastTickDropAt) < time.Second {
			return
		}
		s.lastTickDropAt = now
	case "bar":
		if now.Sub(s.lastBarDropAt) < time.Second {
			return
		}
		s.lastBarDropAt = now
	}
	logger.Warn("market data side effect queue full, dropping event", "kind", kind, "instrument_id", instrumentID)
}
