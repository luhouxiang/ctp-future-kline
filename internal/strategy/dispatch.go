package strategy

import "sync"

type MarketEventSink interface {
	HandleRealtimeTick(TickEvent)
	HandleRealtimeBar(BarEvent)
	HandleReplayTick(TickEvent)
	HandleReplayBar(BarEvent)
}

var (
	defaultSink   MarketEventSink
	defaultSinkMu sync.RWMutex
)

func SetDefaultSink(sink MarketEventSink) {
	defaultSinkMu.Lock()
	defaultSink = sink
	defaultSinkMu.Unlock()
}

func publishTick(ev TickEvent, replay bool) {
	defaultSinkMu.RLock()
	sink := defaultSink
	defaultSinkMu.RUnlock()
	if sink == nil {
		return
	}
	if replay {
		sink.HandleReplayTick(ev)
		return
	}
	sink.HandleRealtimeTick(ev)
}

func publishBar(ev BarEvent, replay bool) {
	defaultSinkMu.RLock()
	sink := defaultSink
	defaultSinkMu.RUnlock()
	if sink == nil {
		return
	}
	if replay {
		sink.HandleReplayBar(ev)
		return
	}
	sink.HandleRealtimeBar(ev)
}

func PublishRealtimeTick(ev TickEvent) { publishTick(ev, false) }
func PublishReplayTick(ev TickEvent)   { publishTick(ev, true) }
func PublishRealtimeBar(ev BarEvent)   { publishBar(ev, false) }
func PublishReplayBar(ev BarEvent)     { publishBar(ev, true) }
