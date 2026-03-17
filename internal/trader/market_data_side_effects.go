package trader

import (
	"sync"
	"time"

	"ctp-go-demo/internal/logger"
)

type marketDataSideEffects struct {
	tickCh chan tickEvent
	barCh  chan minuteBar

	onTick func(tickEvent)
	onBar  func(minuteBar)
	status *RuntimeStatusCenter

	wg sync.WaitGroup

	mu             sync.Mutex
	lastTickDropAt time.Time
	lastBarDropAt  time.Time
}

func newMarketDataSideEffects(status *RuntimeStatusCenter, onTick func(tickEvent), onBar func(minuteBar)) *marketDataSideEffects {
	s := &marketDataSideEffects{
		tickCh: make(chan tickEvent, 16384),
		barCh:  make(chan minuteBar, 4096),
		onTick: onTick,
		onBar:  onBar,
		status: status,
	}
	if onTick != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for ev := range s.tickCh {
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
	default:
		s.logDropLocked("tick", ev.InstrumentID)
	}
}

func (s *marketDataSideEffects) PublishBar(ev minuteBar) {
	if s == nil || s.onBar == nil {
		return
	}
	select {
	case s.barCh <- ev:
	default:
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
